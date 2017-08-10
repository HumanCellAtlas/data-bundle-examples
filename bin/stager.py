#!/usr/bin/env python3.6

import argparse, glob, json, os, signal, ssl, sys
import urllib3
from urllib3.util import parse_url
from shutil import copyfileobj
from concurrent.futures import ProcessPoolExecutor
from checksumming_io.checksumming_io import ChecksummingSink
from utils import logger, sizeof_fmt, measure_duration_and_rate, S3Agent, S3ObjectTagger

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

    def stage_bundles(self, bundles):
        global executor
        signal.signal(signal.SIGINT, self.signal_handler)
        executor = ProcessPoolExecutor(max_workers=self.args.jobs)
        self.total_bundles = len(bundles)
        bundle_number = 0
        for bundle in bundles:
            bundle_number += 1
            executor.submit(self.stage_bundle, bundle, bundle_number)
        executor.shutdown()

    def stage_bundle(self, bundle, bundle_number=None):
        comment = f"({bundle_number}/{self.total_bundles})" if bundle_number else ""
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        BundleStager(self.args.target_bucket).stage(bundle, comment)

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


class BundleStager:

    def __init__(self, target_bucket):
        self.target_bucket = target_bucket

    def stage(self, bundle, comment=""):
        logger.output(f"\nBundle: {bundle.path} {comment}", "B")
        try:
            self._stage_data_files(bundle)
            self._stage_metadata(bundle)
        except BundleMissingDataFile as e:
            logger.output(f" -> {str(e)}\n", "!")
        logger.flush()

    def _stage_data_files(self, bundle):
        if bundle.manifest is None:
            return
        logger.output(f"\n  Data files ({len(bundle.manifest['files'])}):")
        for fileinfo in bundle.manifest['files']:
            file = fileinfo['name']
            logger.output(f"\n    {file} ")
            DataFileStager(bundle, file).stage_file(self.target_bucket)

    def _stage_metadata(self, bundle):
        logger.output("\n  Metadata:")
        for metadata_file in bundle.metadata_files:
            logger.output(f"\n    {metadata_file}: ")
            MetadataFileStager(bundle, metadata_file).stage(self.target_bucket)


class LocalBundle:

    EXAMPLES_ROOT = 'import'
    BUNDLE_HOME_DIRNAME = 'bundles'  # Folders containing bundles
    MANIFEST_FILENAME = 'manifest.json'

    @classmethod
    def bundles_under(cls, folder):
        for bundle_home in cls._find_bundle_homes_under(folder):
            bundle_dirs = os.listdir(bundle_home)
            for bundle_dir in bundle_dirs:
                bundle_path = os.path.join(bundle_home, bundle_dir)
                yield cls(bundle_path)

    def __init__(self, bundle_path):
        self.path = bundle_path
        self.manifest = None
        self.metadata_files = self._find_metadata_files()

    def __lt__(self, other):
        return self.path < other.path

    @classmethod
    def _find_bundle_homes_under(cls, folder):
        for bundle_home in glob.glob(f"{folder}/**/{cls.BUNDLE_HOME_DIRNAME}", recursive=True):
            yield bundle_home

    def _find_metadata_files(self) -> list:
        paths = glob.glob(f"{self.path}/*.json")
        filenames = [os.path.basename(path) for path in paths]
        if self.MANIFEST_FILENAME in filenames:
            manifest_path = f"{self.path}/{self.MANIFEST_FILENAME}"
            with open(manifest_path, 'r') as data:
                self.manifest = json.load(data)
        return filenames


class DataFileStager:

    OLD_BUCKET = 'hca-dss-test-src'

    def __init__(self, bundle, filename):
        self.bundle = bundle
        self.filename = filename
        self.original_location_url = f"{bundle.manifest['dir']}/{filename}"
        self.file_size = self._internet_file_size(self.original_location_url)
        self.target = None
        self.s3 = S3Agent()

    def stage_file(self, target_bucket):
        self.target = f"s3://{target_bucket}/{self.bundle.path}/{self.filename}"
        if self._obj_is_at_target_location():
            logger.progress(",")
        else:
            src_location = self.source_data_file()
            self.copy_file_to_target_location(src_location)
            self._delete_downloaded_file(src_location)
        self._ensure_checksum_tags()

    def _obj_is_at_target_location(self):
        obj = self.s3.get_object(self.target)
        if obj:
            if obj.content_length == self.file_size:
                logger.output("=present ")
                return True
            else:
                logger.output(f"\n      exists at target but has different size: {self.file_size} / {obj.content_length}")
        return False

    def source_data_file(self):
        location = self._find_locally() or self._download_from_source()
        if location:
            logger.output(f"\n      found at {location}")
            return location
        raise BundleMissingDataFile(f"Cannot find source for {self.filename}")

    def copy_file_to_target_location(self, source_location):
        if parse_url(source_location).scheme == 's3':
            logger.output(f"\n      copy to {self.target} ", "C")
            report_duration_and_rate(self.s3.copy_between_buckets,
                                     source_location,
                                     self.target,
                                     self.file_size,
                                     size=self.file_size)
            S3ObjectTagger(self.target).copy_tags_from_object(source_location)
        elif parse_url(source_location).scheme == 'file':
            local_path = parse_url(source_location).path.lstrip('/')
            logger.output(f"\n      upload to {self.target} ", "^")
            checksums = report_duration_and_rate(self.s3.upload_and_checksum,
                                                 local_path,
                                                 self.target,
                                                 self.file_size,
                                                 size=self.file_size)
            S3ObjectTagger(self.target).tag_using_these_checksums(checksums)
            logger.output("+tagging ")
        else:
            raise RuntimeError(f"Unrecognized scheme: {source_location}")

    def _find_locally(self):
        local_path = os.path.join(self.bundle.path, self.filename)
        if os.path.isfile(local_path) and os.stat(local_path).st_size == self.file_size:
            return f"file:///{local_path}"
        return None

    def _download_from_source(self):
        logger.output(f"\n      downloading {self.original_location_url} [{sizeof_fmt(self.file_size)}]", "v")
        dest_path = os.path.join(self.bundle.path, self.filename)
        try:
            report_duration_and_rate(self._download, self.original_location_url, dest_path, size=self.file_size)
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
            with http.request('GET', src_url, preload_content=False) as in_stream:
                copyfileobj(in_stream, out_file)

    @staticmethod
    def _internet_file_size(url: str) -> int:
        return int(http.request('HEAD', url).headers['Content-Length'])


class MetadataFileStager:

    def __init__(self, bundle: LocalBundle, metadata_filename: str):
        self.bundle = bundle
        self.filename = metadata_filename
        self.file_path = f"{self.bundle.path}/{self.filename}"
        self.file_size = os.stat(self.file_path).st_size
        self.s3 = S3Agent()

    def stage(self, bucket) -> bool:
        self.target = f"s3://{bucket}/{self.file_path}"
        if self._obj_is_at_target_location():
            logger.output("=present ", progress_char=".")
            S3ObjectTagger(self.target).complete_tags()
        else:
            logger.output("+uploading ", progress_char="u")
            checksums = self.s3.upload_and_checksum(self.file_path, self.target, self.file_size)
            S3ObjectTagger(self.target).tag_using_these_checksums(checksums)
            logger.output("+tagging ")

    def _obj_is_at_target_location(self):
        obj = self.s3.get_object(self.target)
        if obj:
            local_checksums = self._checksum_local_file()
            if local_checksums['s3_etag'] == obj.e_tag.strip('"'):
                return True
            else:
                logger.output(f"\n      exists at target but has a different ETAG ")
        return False

    def _checksum_local_file(self):
        with ChecksummingSink() as sink:
            with open(self.file_path, 'rb') as fh:
                copyfileobj(fh, sink)
            return sink.get_checksums()


# run the class
if __name__ == '__main__':
    runner = Main()
