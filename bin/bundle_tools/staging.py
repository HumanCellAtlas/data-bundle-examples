import os
import urllib3
from urllib3.util import parse_url
from shutil import copyfileobj
from checksumming_io.checksumming_io import ChecksummingSink
from utils import logger, sizeof_fmt, measure_duration_and_rate, S3Agent, S3ObjectTagger
from bundle_tools import LocalBundle, File, DataFile, MetadataFile, SubmissionInfo


class BundleMissingDataFile(Exception):
    pass


def report_duration_and_rate(func,  *args, size):
    retval, duration, rate = measure_duration_and_rate(func, *args, size=size)
    logger.output(" (%.1f sec, %.1f MiB/sec)" % (duration, rate))
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
            logger.output(f"\n    {file.name} ")
            if type(file) == DataFile:
                DataFileStager(file).stage_file(self.target_bucket)
            else:
                MetadataFileStager(file).stage(self.target_bucket)


class DataFileStager:

    def __init__(self, file):
        self.file = file
        self.bundle = file.bundle
        self.target = None

    def stage_file(self, target_bucket):
        self.target = f"s3://{target_bucket}/{self.file.path()}"
        if self._obj_is_at_target_location():
            logger.progress(",")
        else:
            src_location = self.source_data_file()
            self.copy_file_to_target_location(src_location)
            self._delete_downloaded_file(src_location)
        self.file.staged_url = self.target
        self._ensure_checksum_tags()

    def _obj_is_at_target_location(self):
        obj = s3.get_object(self.target)
        if obj:
            if obj.content_length == self.file.size:
                logger.output("=present ")
                return True
            else:
                logger.output(f"\n      exists at target but has different size: {self.file.size} / {obj.content_length}")
        return False

    def source_data_file(self):
        location = self._find_locally() or self._download_from_source()
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
        logger.output(f"\n      copy to {self.target} ", "C")
        report_duration_and_rate(s3.copy_between_buckets,
                                 source_location,
                                 self.target,
                                 self.file.size,
                                 size=self.file.size)
        S3ObjectTagger(self.target).copy_tags_from_object(source_location)

    def copy_local_file_to_target_location(self, source_location):
        local_path = parse_url(source_location).path.lstrip('/')
        logger.output(f"\n      upload to {self.target} ", "^")
        self.file.checksums = report_duration_and_rate(s3.upload_and_checksum,
                                                       local_path,
                                                       self.target,
                                                       self.file.size,
                                                       size=self.file.size)
        S3ObjectTagger(self.target).tag_using_these_checksums(self.file.checksums)
        logger.output("+tagging ")

    def _find_locally(self):
        local_path = self.file.path()
        if os.path.isfile(local_path) and os.stat(local_path).st_size == self.file.size:
            return f"file:///{local_path}"
        return None

    def _download_from_source(self):
        logger.output(f"\n      downloading {self.file.origin_url} [{sizeof_fmt(self.file.size)}]", "v")
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
        if S3ObjectTagger(self.target).complete_tags():
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
            logger.output("=present ", progress_char=".")
            S3ObjectTagger(self.target).complete_tags()
        else:
            logger.output("+uploading ", progress_char="u")
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

