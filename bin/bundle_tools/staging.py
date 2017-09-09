import os
from shutil import copyfileobj

import urllib3
from urllib3.util import parse_url
from checksumming_io.checksumming_io import ChecksummingSink

from .parallel_logger import logger
from .utils import sizeof_fmt, measure_duration_and_rate
from .s3 import S3Location, S3Agent, S3ObjectTagger
from bundle_tools import LocalBundle, File, DataFile, MetadataFile, SubmissionInfo


class BundleMissingDataFile(Exception):
    pass


def report_duration_and_rate(func,  *args, size):
    retval, duration, rate = measure_duration_and_rate(func, *args, size=size)
    logger.output(" (%.1f sec, %.1f MiB/sec) " % (duration, rate))
    return retval


s3 = S3Agent()
http = urllib3.PoolManager()


class BundleStager:

    def __init__(self, bundle: LocalBundle, target_bucket: str):
        self.bundle = bundle
        self.target_bucket = target_bucket

    def stage(self, comment=""):
        logger.output(f"\nBundle: {self.bundle.path} {comment}", "B")
        try:
            self.bundle.submission_info = SubmissionInfo(self.target_bucket, self.bundle)
            self.bundle.submission_info.load()
            self.bundle.enumerate_local_metadata_files()
            self.bundle.enumerate_data_files_using_manifest()
            self._stage_files_of_type(DataFile)
            self._stage_files_of_type(MetadataFile)
            if self.bundle.submission_info.save():
                logger.output("\n  Writing submission.json", progress_char='*')

        except BundleMissingDataFile as e:
            logger.output(f" -> {str(e)}\n", "!")
        logger.flush()

    def _stage_files_of_type(self, file_class):
        files = [file for file in self.bundle.files.values() if type(file) == file_class]
        logger.output(f"\n  {file_class.__name__}s ({len(files)}):")
        for file in files:
            logger.output(f"\n    {file.name} ({sizeof_fmt(file.size)}) ")
            if type(file) == DataFile:
                DataFileStager(file).stage_file(self.target_bucket)
            else:
                MetadataFileStager(file).stage(self.target_bucket)


class DataFileStager:

    def __init__(self, file):
        self.file = file
        self.bundle = file.bundle
        self.target_url = None

    def stage_file(self, target_bucket):
        self.target_url = f"s3://{target_bucket}/{self.file.path()}"
        if self._obj_is_at_target_location():
            logger.output("=present ", progress_char="✔︎")
        else:
            src_location = self.source_data_file()
            self.copy_file_to_target_location(src_location)
            self._delete_downloaded_file(src_location)
        self.file.staged_url = self.target_url
        self._ensure_checksum_tags()

    def _obj_is_at_target_location(self):
        obj = s3.get_object(self.target_url)
        if obj:
            if obj.content_length == self.file.size:
                return self.etag_matches_or_not_present(obj)
            else:
                logger.output("\n      exists at target but has different size: %d / %d" %
                              (self.file.size, obj.content_length))
        return False

    def etag_matches_or_not_present(self, obj):
        s3_etag = obj.e_tag.strip('"')
        tags = s3.get_tagging(self.target_url)
        if tags.get('hca-dss-s3_etag'):
            if s3_etag == tags['hca-dss-s3_etag']:
                return True
            else:
                logger.output("\n      exists at target but has wrong etag: %s / %s" %
                              (s3_etag, tags['hca-dss-s3_etag']))
                logger.output("\n      copy to itself to correct etag... ", progress_char='↻︎')
                report_duration_and_rate(s3.copy_between_buckets,
                                         self.target_url, self.target_url, self.file.size,
                                         size=self.file.size)
                return True
        else:
            # File size matches but file has no checksum tag.
            # Assume file is good and proceed so we compute new checksums.
            return True

    def source_data_file(self):
        location = self._find_locally() or self._download_from_origin()
        if location:
            logger.output(f"\n      found at {location}")
            return location
        raise BundleMissingDataFile(f"Cannot find source for {self.file.name}")

    def copy_file_to_target_location(self, source_location):
        if parse_url(source_location).scheme == 's3':
            self.copy_s3_file_to_target_location(source_location)
        elif parse_url(source_location).scheme == 'file':
            self.copy_local_file_to_target_location(source_location)
        else:
            raise RuntimeError(f"Unrecognized scheme: {source_location}")

    def copy_s3_file_to_target_location(self, source_location):
        logger.output(f"\n      copy to {self.target_url} ", "C")
        report_duration_and_rate(s3.copy_between_buckets,
                                 source_location,
                                 self.target_url,
                                 self.file.size,
                                 size=self.file.size)
        S3ObjectTagger(self.target_url).complete_tags()

    def copy_local_file_to_target_location(self, source_location):
        local_path = parse_url(source_location).path.lstrip('/')
        logger.output(f"\n      upload to {self.target_url} ", "⬆︎")
        self.file.checksums = report_duration_and_rate(s3.upload_and_checksum,
                                                       local_path,
                                                       self.target_url,
                                                       self.file.size,
                                                       size=self.file.size)
        S3ObjectTagger(self.target_url).tag_using_these_checksums(self.file.checksums)
        logger.output("+tagging ")

    def _find_locally(self):
        local_path = self.file.path()
        if os.path.isfile(local_path) and os.stat(local_path).st_size == self.file.size:
            return f"file:///{local_path}"
        return None

    def _download_from_origin(self):
        logger.output(f"\n      downloading {self.file.origin_url}", "↓")
        dest_path = self.file.path()
        try:
            report_duration_and_rate(self._download, self.file.origin_url, dest_path, size=self.file.size)
            return f"file:///{dest_path}"
        # except urllib.error.HTTPError:
        except Exception as e:
            logger.output(f"      error downloading ({str(e)})", "!")
            os.remove(dest_path)
            return None

    def _ensure_checksum_tags(self):
        if S3ObjectTagger(self.target_url).complete_tags():
            logger.progress("+")

    @staticmethod
    def _delete_downloaded_file(location):
        urlbits = parse_url(location)
        if urlbits.scheme == 'file':
            logger.output(f"\n      Deleting {location}")
            os.remove(urlbits.path.lstrip('/'))

    @staticmethod
    def _download(src_url: str, dest_path: str):
        with open(dest_path, 'wb') as out_file:
            # TODO now that we switched from urlopen(), this will fail with FTP files
            with http.request('GET', src_url, preload_content=False) as in_stream:
                copyfileobj(in_stream, out_file)


class MetadataFileStager:

    def __init__(self, file: File):
        self.file = file
        self.bundle = file.bundle

    def stage(self, bucket):
        self.target = f"s3://{bucket}/{self.file.path()}"
        if self._obj_is_at_target_location():
            logger.output("=present ", progress_char="✓")
            S3ObjectTagger(self.target).complete_tags()
        else:
            logger.output("+uploading ", progress_char="↑")
            checksums = s3.upload_and_checksum(self.file.path(), self.target, self.file.size)
            S3ObjectTagger(self.target).tag_using_these_checksums(checksums)
            logger.output("+tagging ")
        self.file.staged_url = self.target

    def _obj_is_at_target_location(self):
        obj = s3.get_object(self.target)
        if obj:
            local_checksums = self._checksum_local_file()
            if local_checksums['s3_etag'] == obj.e_tag.strip('"'):
                return True
            else:
                logger.output(f"\n      exists at target but has a different ETAG ")
        return False

    def _checksum_local_file(self):
        with ChecksummingSink() as sink:
            with open(self.file.path(), 'rb') as fh:
                copyfileobj(fh, sink)
            return sink.get_checksums()

