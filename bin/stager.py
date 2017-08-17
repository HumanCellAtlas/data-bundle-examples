#!/usr/bin/env python3.6

import argparse, copy, glob, json, os, re, signal, ssl, sys
import urllib3
from urllib3.util import parse_url
from shutil import copyfileobj
from concurrent.futures import ProcessPoolExecutor
from checksumming_io.checksumming_io import ChecksummingSink
from utils import logger, sizeof_fmt, measure_duration_and_rate, S3Agent, S3ObjectTagger
from botocore.exceptions import ClientError
from ftplib import FTP

"""
    stager.py - Stage Example Data Bundles in S3 Bucket org-humancellatlas-data-bundle-examples
    
    Un-tar metadata files before running: tar xf import/import.tgz
    
    Default action is to traverse the import/ folder finding bundles, then for each bundle:
        For each data file:
            - Find the original (using manifest.json) and note its size.
            - If this version is not at the desired location, download it, then upload it to S3.
        For each metadata file:
            - Checksum it.
            - Upload to S3 unless a files already exists there with this checksum.
            
    Checking 100,000 files can be a slow process, so you can parallelize with the -j option.
    Try running on an m4.2xlarge with -j16.  This will take under an hour and works well in
    the case where there are no new data-files to be uploaded.  Note however that if there
    are new data-files to be uploaded, you will want to use minimal or no concurrency for
    those bundles to avoid overloading the web server from which they are being downloaded.
    
    When running parallelized, terse output will be produced.
    
    Terse output key:
    
        B - a new bundle is being examined
        , - a data file has been checked and is already in place
        ! - a data file could not be found
        C - a data file was copied from another S3 bucket to the target location
        v - a data file was downloaded from the internet
        ^ - a data file was upload to the target bucket
        + - missing checksums where added to an already uploaded file 
        . - a metadata file has been checked and is already in place
        u - a metadata file was uploaded to the target location
        
        e.g. this bundle is already done: B,.....
             this bundle was new:         Bv^uuuuu
    
    When running parallelized you can still generate verbose output with the --log option. 
"""


class BundleMissingDataFile(Exception):
    pass


def report_duration_and_rate(func,  *args, size):
    retval, duration, rate = measure_duration_and_rate(func, *args, size=size)
    logger.output(" (%.1f sec, %.1f MiB/sec)" % (duration, rate))
    return retval


# Executor complains if it is an object attribute, so we make it global.
executor = None
http = urllib3.PoolManager()
s3 = S3Agent()


class Main:

    DEFAULT_BUCKET = 'org-humancellatlas-data-bundle-examples'

    def __init__(self):
        self._parse_args()
        self._setup_ssl_context(self.args.skip_ssl_cert_verification)
        if self.args.bundle:
            self.stage_bundle(LocalBundle(self.args.bundle))
        else:
            logger.output(f"\nStaging bundles under \"{self.args.bundles}\":\n")
            bundles = list(LocalBundle.bundles_under(self.args.bundles))
            bundles.sort()
            self.stage_bundles(bundles)
        print("")

    def stage_bundles(self, bundles):
        self.total_bundles = len(bundles)
        if self.args.jobs > 1:
            self.stage_bundles_in_parallel(bundles)
        else:
            self.stage_bundles_serially(bundles)

    def stage_bundles_serially(self, bundles):
        """ This produces much better error messages that operating under ProcessPoolExecutor """
        bundle_number = 0
        for bundle in bundles:
            bundle_number += 1
            self.stage_bundle(bundle, bundle_number)

    def stage_bundles_in_parallel(self, bundles):
        global executor
        signal.signal(signal.SIGINT, self.signal_handler)
        executor = ProcessPoolExecutor(max_workers=self.args.jobs)
        bundle_number = 0
        for bundle in bundles:
            bundle_number += 1
            executor.submit(self.stage_bundle, bundle, bundle_number)
        executor.shutdown()

    def stage_bundle(self, bundle, bundle_number=None):
        comment = f"({bundle_number}/{self.total_bundles})" if bundle_number else ""
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        BundleStager(bundle, self.args.target_bucket).stage(comment)

    def _parse_args(self):
        parser = argparse.ArgumentParser(description="Stage example bundles in S3.",
                                         usage='%(prog)s [options]',
                                         epilog="Default action is to stage all bundles under ./import/")
        parser.add_argument('--target-bucket', metavar="<s3-bucket-name>", default=self.DEFAULT_BUCKET,
                            help="stage files in this bucket")
        parser.add_argument('--bundle', default=None, metavar="path/to/bundle",
                            help="stage single bundle at this path")
        parser.add_argument('--bundles', default='import', metavar="path",
                            help="stage bundles under this path (must not include 'bundles')")
        parser.add_argument('-q', '--quiet', action='store_true', default=False,
                            help="silence is golden")
        parser.add_argument('-t', '--terse', action='store_true', default=False,
                            help="terse output, one character per file")
        parser.add_argument('-l', '--log', default=None,
                            help="log verbose output to this file")
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help="parallelize with this many jobs")
        parser.add_argument('--skip-ssl-cert-verification', default=False, action='store_true',
                            help="don't attempt to verify SSL certificates")
        self.args = parser.parse_args()
        if self.args.jobs > 1:
            quiet = True
            terse = True
        else:
            quiet = self.args.quiet
            terse = self.args.terse

        logger.configure(self.args.log, quiet=quiet, terse=terse)

    @staticmethod
    def signal_handler(signal, frame):
        global executor
        print('Shutting down...')
        executor.shutdown()
        sys.exit(0)

    @staticmethod
    def _setup_ssl_context(skip_ssl_cert_verification):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE if skip_ssl_cert_verification else ssl.CERT_REQUIRED


class File:
    def __init__(self, name=None, bundle=None, uuid=None, size=None, content_type=None, origin_url=None, staged_url=None):
        self.name = name
        self.bundle = bundle
        self.uuid = uuid
        self.size = size
        self.content_type = content_type
        self.origin_url = origin_url
        self.staged_url = staged_url
        self.checksums = {}

    def __eq__(self, other) -> bool:
        return self.bundle == other.bundle and self.name == other.name

    def path(self):
        return f"{self.bundle.path}/{self.name}"


class MetadataFile(File):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content_type = self.content_type or 'application/json'


class DataFile(File):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content_type = self.content_type or 'application/octet-stream'


class SubmissionInfo:

    SUBMISSION_FILENAME = "submission.json"

    def __init__(self, bucket_name, bundle):
        self.bucket_name = bucket_name
        self.bundle = bundle
        self.info = None
        self.orig_info = None
        self.s3_obj = s3.object(f"s3://{self.bucket_name}/{self.bundle.path}/{self.SUBMISSION_FILENAME}")
        pass

    def load(self):
        try:
            submission_json = self.s3_obj.get()['Body'].read()
            self.info = json.loads(submission_json)
            self.orig_info = copy.deepcopy(self.info)
            for file in self.export_files():
                self.bundle.add_file(file)
        except ClientError:
            self.info = {}

    def save(self):
        self.extract_bundle_info()
        if self.info != self.orig_info:
            logger.output("\n  Writing submission.json", progress_char='*')
            self.s3_obj.put(Body=json.dumps(self.info))

    def export_files(self):
        return map(self._convert_file_entry_to_file, self.info['files'])

    def extract_bundle_info(self):
        file_entries = self.info.setdefault('files', [])
        for file in self.bundle.files.values():
            try:
                file_entry = next((item for item in file_entries if item["name"] == file.name))
            except StopIteration:
                file_entry = dict()
                file_entries.append(file_entry)
            self.update_file_entry(file, file_entry)

    def update_file_entry(self, file: File, entry: dict):
        entry['name'] = file.name
        entry['content_type'] = file.content_type
        entry['size'] = file.size
        entry['staged_url'] = file.staged_url
        if file.origin_url:
            entry['origin_url'] = file.origin_url

    def _convert_file_entry_to_file(self, file_entry):
        if re.match(".*\.json", file_entry['name']):
            return MetadataFile(**file_entry)
        else:
            return DataFile(**file_entry)


class Bundle:

    def __init__(self, path=None, staged_url=None):
        self.path = path
        self.staged_url = staged_url
        self.files = dict()
        self.submission_info = None

    def __lt__(self, other):
        return self.path < other.path

    def add_file(self, file: File):
        file.bundle = self
        self.files[file.name] = file


class LocalBundle(Bundle):

    MANIFEST_FILENAME = 'manifest.json'
    BUNDLE_HOME_DIRNAME = 'bundles'  # Folders containing bundles

    @classmethod
    def bundles_under(cls, folder):
        for bundle_home in cls._find_bundle_homes_under(folder):
            bundle_dirs = os.listdir(bundle_home)
            for bundle_dir in bundle_dirs:
                bundle_path = os.path.join(bundle_home, bundle_dir)
                yield cls(bundle_path)

    @classmethod
    def _find_bundle_homes_under(cls, folder):
        for bundle_home in glob.glob(f"{folder}/**/{cls.BUNDLE_HOME_DIRNAME}", recursive=True):
            yield bundle_home

    def __init__(self, local_path):
        super().__init__(path=local_path)
        self.manifest = None

    def enumerate_local_metadata_files(self):
        for path in glob.glob(f"{self.path}/*.json"):
            name = os.path.basename(path)
            if name == self.MANIFEST_FILENAME:
                self.manifest = path
            else:
                if name not in self.files:
                    self.add_file(MetadataFile(name=name, size=os.stat(path).st_size))

    def enumerate_data_files_using_manifest(self):
        if self.manifest is None:
            raise RuntimeError(f"Bundle {self.path} has no {self.MANIFEST_FILENAME}")
        with open(self.manifest, 'r') as data:
            manifest = json.load(data)
            for fileinfo in manifest['files']:
                origin_url = f"{manifest['dir']}/{fileinfo['name']}"
                size = self._internet_file_size(origin_url)
                if fileinfo['name'] not in self.files:
                    self.add_file(DataFile(name=fileinfo['name'], size=size, origin_url=origin_url))

    @staticmethod
    def _internet_file_size(url: str) -> int:
        urlbits = parse_url(url)
        if urlbits.scheme == 'http':
            return int(http.request('HEAD', url).headers['Content-Length'])
        elif urlbits.scheme == 'ftp':
            return LocalBundle._ftp_file_size(urlbits)
        else:
            raise RuntimeError(f"Odd scheme: {urlbits.scheme} for original file: {url}")

    @staticmethod
    def _ftp_file_size(ftp_url):
        ftp = FTP(ftp_url.netloc)
        ftp.login()
        size = ftp.size(ftp_url.path)
        ftp.quit()
        return size


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
            self.bundle.submission_info.save()
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


# run the class
if __name__ == '__main__':
    runner = Main()
