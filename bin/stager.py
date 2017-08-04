#!/usr/bin/env python3.6

import argparse, glob, json, io, mimetypes, os, re, signal, ssl, sys
from urllib.request import urlopen, Request
from shutil import copyfileobj
from functools import reduce
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import boto3
from botocore.exceptions import ClientError
from dotmap import DotMap

from checksumming_io.checksumming_io import ChecksummingBufferedReader, ChecksummingSink


def sizeof_fmt(num, suffix='B'):
    """
    From https://stackoverflow.com/a/1094933
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)


class BundleMissingDataFile(Exception):
    pass


class S3Location(DotMap):

    def __init__(self, bucket: str, key: str):
        super().__init__(Bucket=bucket, Key=key)

    def __str__(self):
        return f"s3://{self.Bucket}/{self.Key}"


class MyLogger:
    def __init__(self):
        self.destination = sys.stdout
        self.progress_string = ""
        self.terse = False
        self.quiet = False
        self.logfile = None

    def configure(self, logfile=None, terse=False, quiet=False):
        self.terse = terse
        self.quiet = quiet
        if logfile:
            self.logfile = io.TextIOWrapper(open(logfile, 'wb'))

    def output(self, msg, progress_char=None):
        if not self.quiet:
            sys.stdout.write(msg)
        if self.logfile:
            self.logfile.write(msg)
        if progress_char:
            self.progress(progress_char)

    def progress(self, char):
        self.progress_string += char

    def flush(self):
        if self.terse:
            sys.stdout.write(self.progress_string)
        self.progress_string = ""
        sys.stdout.flush()
        if self.logfile:
            self.logfile.flush()


logger = MyLogger()


# Executor complains if it is an object attribute, so we make it global.
executor = None


class Main:

    DEFAULT_BUCKET = 'org-humancellatlas-data-bundle-examples'

    def __init__(self):
        self._parse_args()
        if self.args.bundle:
            self.stage_bundle(LocalBundle(self.args.bundle))
        else:
            bundles = list(LocalBundle.bundles_under(self.args.bundles))
            bundles.sort()
            self.stage_bundles(bundles)
        print("")

    def _parse_args(self):
        parser = argparse.ArgumentParser(description="Stage example bundles in S3",
                                         usage='%(prog)s [options]')
        parser.add_argument('--target-bucket', default=self.DEFAULT_BUCKET, help="Stage files in this bucket")
        parser.add_argument('--bundle', default=None, help="Stage single bundle at this path")
        parser.add_argument('--bundles', default='import',
                            help="Stage bundles under this path (must not include 'bundles')")
        parser.add_argument('-q', '--quiet', action='store_true', default=False, help="Silence is golden")
        parser.add_argument('-t', '--terse', action='store_true', default=False, help="Terse output, one dot per bundle")
        parser.add_argument('-l', '--log', default=None, help="Log verbose output to this file")
        parser.add_argument('-j', '--jobs', type=int, default=1, help="Parallelize with this many jobs")
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
        comment = f"({bundle_number}/{self.total_bundles})" if bundle_number else None
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        BundleStager(self.args.target_bucket).stage(bundle, comment)

    @staticmethod
    def signal_handler(signal, frame):
        global executor
        print('Shutting down...')
        executor.shutdown()
        sys.exit(0)


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
            # TODO update assay.json with data file locations?

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
        self.target = None
        self.s3 = S3Agent()

    def stage_file(self, target_bucket):
        self.target = S3Location(bucket=target_bucket, key=f"{self.bundle.path}/{self.filename}")
        if self._obj_is_at_target_location():
            logger.progress(",")
        else:
            location = self.source_data_file()
            self.copy_file_to_target_location(location)
            self._delete_file(location)
        self._ensure_checksum_tags()

    def _obj_is_at_target_location(self):
        if self.s3.get_object(self.target):
            # TODO also check size or checksum
            logger.output("=present ")
            return True

    def source_data_file(self):
        location = self._find_in_s3() or self._find_locally() or self._download_from_source()
        if location:
            logger.output(f"\n      found at {location}")
            return location
        raise BundleMissingDataFile(f"Cannot find source for {self.filename}")

    def copy_file_to_target_location(self, source_location):
        if type(source_location) is S3Location:
            logger.output(f"\n      copy to {self.target}", "C")
            self.s3.copy_between_buckets(source_location, self.target)
            S3ObjectTagger(self.target).copy_tags_from_object(source_location)
        else:
            logger.output(f"\n      upload to {self.target}", "^")
            self.s3.upload_with_checksum_tags(source_location, self.target)

    def _find_in_s3(self):
        """
        Try
            s3://hca-dss-test-src/data-bundles-examples/import/xx/yy/bundles/bundleX/file
            s3://hca-dss-test-src/data-bundles-examples/import/xx/yy/fastqs/file
        and s3://hca-dss-test-src/data-bundles-examples/import/geo/GSExxxxx/sra/SRRxxxxx/file if it is a geo
        """
        trim_from_bundles = re.search('(.*)/bundles/.*$', self.bundle.path)[1]
        potential_locations = [
            S3Location(bucket=self.OLD_BUCKET, key=f"data-bundles-examples/{self.bundle.path}/{self.filename}"),
            S3Location(bucket=self.OLD_BUCKET, key=f"data-bundles-examples/{trim_from_bundles}/fastqs/{self.filename}")
        ]
        if re.search("/geo/", self.bundle.path):
            srr_id = re.search("(SRR\d+)_.*", self.filename)[1]
            potential_locations.append(
                S3Location(bucket=self.OLD_BUCKET, key=f"data-bundles-examples/{trim_from_bundles}/sra/{srr_id}/{self.filename}")
            )
        for loc in potential_locations:
            if self.s3.get_object(loc):
                return loc
        return None

    def _find_locally(self):
        local_path = os.path.join(self.bundle.path, self.filename)
        if os.path.isfile(local_path):
            return local_path
        return None

    def _download_from_source(self):
        src_url = f"{self.bundle.manifest['dir']}/{self.filename}"
        size = self._internet_file_size(src_url)
        logger.output(f"\n      downloading {src_url} [{sizeof_fmt(int(size))}]", "v")
        dest_path = os.path.join(self.bundle.path, self.filename)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        try:
            with urlopen(src_url) as in_stream, open(dest_path, 'wb') as out_file:
                copyfileobj(in_stream, out_file)
            return dest_path
        # except urllib.error.HTTPError:
        except Exception as e:
            logger.output(f"      error downloading ({str(e)})", "!")
            os.remove(dest_path)
            return None

    def _ensure_checksum_tags(self):
        if S3ObjectTagger(self.target).complete_tags():
            logger.progress("+")

    def _delete_file(self, location):
        if type(location) == S3Location:
            logger.output(f"\n      Deleting {location}")
            self.s3.delete_object(location)
        else:
            os.remove(location)

    @staticmethod
    def _internet_file_size(url):
            request = Request(url, method='HEAD')
            response = urlopen(request)
            return response.headers['Content-Length']


class MetadataFileStager:

    def __init__(self, bundle: LocalBundle, metadata_filename: str):
        self.bundle = bundle
        self.filename = metadata_filename
        self.file_path = f"{self.bundle.path}/{self.filename}"
        self.s3 = S3Agent()

    def stage(self, bucket) -> bool:
        target_location = S3Location(bucket, self.file_path)
        if self.s3.get_object(target_location):
            # TODO also check size or checksum
            logger.output("=present ", progress_char=".")
            S3ObjectTagger(target_location).complete_tags()
        else:
            logger.output("+uploading ", progress_char="u")
            self.s3.upload_with_checksum_tags(self.file_path, target_location)


class S3ObjectTagger:
    """
    Tag existing S3 objects
    """

    CHECKSUM_TAGS = ('hca-dss-sha1', 'hca-dss-sha256', 'hca-dss-crc32c', 'hca-dss-s3_etag')
    MIME_TAG = 'hca-dss-content-type'
    ALL_TAGS = CHECKSUM_TAGS + (MIME_TAG,)

    def __init__(self, target: S3Location):
        self.target = target
        self.s3 = S3Agent()

    def copy_tags_from_object(self, src: S3Location):
        self.s3.copy_object_tagging(src, self.target)
        self.complete_tags()

    def tag_using_these_checksums(self, raw_sums: dict):
        tags = self._hca_checksum_tags(raw_sums)
        tags.update(self._generate_mime_tags())
        logger.output("+tagging ")
        self.s3.add_tagging(self.target, tags)

    def complete_tags(self):
        current_tags = self.s3.get_tagging(self.target)
        missing_tags = self._missing_tags(current_tags, self.ALL_TAGS)
        if missing_tags:
            logger.output(f"\n      missing tags: {missing_tags}")
            if self._missing_tags(current_tags, self.CHECKSUM_TAGS):
                current_tags.update(self._generate_checksum_tags())
            if self.MIME_TAG not in current_tags:
                current_tags.update(self._generate_mime_tags())
            logger.output(f"\n      Tagging with: {list(current_tags.keys())}")
            self.s3.add_tagging(self.target, current_tags)
            return True
        return False

    def _generate_checksum_tags(self) -> dict:
        logger.output("\n      generating checksums")
        sums = self._compute_checksums_from_s3(self.target)
        return self._hca_checksum_tags(sums)

    def _generate_mime_tags(self) -> dict:
        mime_type = mimetypes.guess_type(self.target.Key)[0]
        if mime_type is None:
            mime_type = "application/octet-stream"
        return {self.MIME_TAG: mime_type}

    def _compute_checksums_from_s3(self, s3loc: S3Location) -> dict:
        with ChecksummingSink() as sink:
            self.s3.s3client.download_fileobj(s3loc.Bucket, s3loc.Key, sink)
            return sink.get_checksums()

    @staticmethod
    def _hca_checksum_tags(checksums: dict) -> dict:
        return {
            'hca-dss-s3_etag': checksums['s3_etag'],
            'hca-dss-sha1':    checksums['sha1'],
            'hca-dss-sha256':  checksums['sha256'],
            'hca-dss-crc32c':  checksums['crc32c'],
        }

    @staticmethod
    def _missing_tags(actual_tags: dict, desired_tags: tuple) -> list:
        return list(filter(lambda tag: tag not in actual_tags.keys(), desired_tags))


class S3Agent:

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.s3client = self.s3.meta.client

    def copy_between_buckets(self, src: S3Location, dest: S3Location):
        obj = self.s3.Bucket(dest.Bucket).Object(dest.Key)
        obj.copy(src.toDict(), ExtraArgs={'ACL': 'bucket-owner-full-control'})

    def upload_with_checksum_tags(self, local_path: str, target: S3Location):
        bucket = self.s3.Bucket(target.Bucket)
        with open(local_path, 'rb') as fh:
            reader = ChecksummingBufferedReader(fh)
            obj = bucket.Object(target.Key)
            obj.upload_fileobj(reader, ExtraArgs={'ACL': 'bucket-owner-full-control'})
        sums = reader.get_checksums()
        S3ObjectTagger(target).tag_using_these_checksums(sums)

    def copy_object_tagging(self, src: S3Location, dest: S3Location):
        tags = self.get_tagging(src)
        self.add_tagging(dest, tags)

    def get_tagging(self, s3loc: S3Location) -> dict:
        response = self.s3client.get_object_tagging(Bucket=s3loc.Bucket, Key=s3loc.Key)
        return self._decode_tags(response['TagSet'])

    def add_tagging(self, s3loc: S3Location, tags: dict):
        tagging = dict(TagSet=self._encode_tags(tags))
        self.s3client.put_object_tagging(Bucket=s3loc.Bucket, Key=s3loc.Key, Tagging=tagging)

    def get_object(self, s3loc: S3Location):
        try:
            obj = self.s3.Object(s3loc.Bucket, s3loc.Key)
            obj.load()
            return obj
        except ClientError:
            return None

    def delete_object(self, s3loc: S3Location):
        self.s3.Bucket(s3loc.Bucket).Object(s3loc.Key).delete()

    @staticmethod
    def _decode_tags(tags: list) -> dict:
        if not tags:
            return {}
        simplified_dicts = list({tag['Key']: tag['Value']} for tag in tags)
        return reduce(lambda x, y: dict(x, **y), simplified_dicts)

    @staticmethod
    def _encode_tags(tags: dict) -> list:
        return [dict(Key=k, Value=v) for k, v in tags.items()]


# run the class
if __name__ == '__main__':
    runner = Main()
