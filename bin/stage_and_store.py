#!/usr/bin/env python

"""
stage_and_story.py - Stage a bundle and store it in the HCA DSS.
"""

import argparse, glob, os, io, sys, re, time, logging, uuid, json, base64, hashlib, mimetypes
from io import BufferedReader
from datetime import datetime

try:
    import boto3, requests, crcmod
    from boto3.s3.transfer import TransferConfig
    from dotmap import DotMap
    from urllib3.util import parse_url
except ImportError:
    print("\nPlease install the following packages use this script:\n" +
          "\n\tpip install boto3 requests crcmod dotmap urllib3\n")
    exit(1)

logging.basicConfig()
logger = logging.getLogger("stage_and_store")

KB = 1024
MB = KB * KB


def sizeof_fmt(num, suffix='B'):
    """
    From https://stackoverflow.com/a/1094933
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%d %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)


def progress(message):
    sys.stdout.write(message)
    sys.stdout.flush()


class S3Etag:
    etag_stride = 64 * 1024 * 1024

    def __init__(self):
        self._etag_bytes = 0
        self._etag_parts = []
        self._etag_hasher = hashlib.md5()

    def update(self, chunk):
        if self._etag_bytes + len(chunk) > self.etag_stride:
            chunk_head = chunk[:self.etag_stride - self._etag_bytes]
            chunk_tail = chunk[self.etag_stride - self._etag_bytes:]
            self._etag_hasher.update(chunk_head)
            self._etag_parts.append(self._etag_hasher.digest())
            self._etag_hasher = hashlib.md5()
            self._etag_hasher.update(chunk_tail)
            self._etag_bytes = len(chunk_tail)
        else:
            self._etag_hasher.update(chunk)
            self._etag_bytes += len(chunk)

    def hexdigest(self):
        if self._etag_bytes:
            self._etag_parts.append(self._etag_hasher.digest())
            self._etag_bytes = 0
        if len(self._etag_parts) > 1:
            etag_csum = hashlib.md5(b"".join(self._etag_parts)).hexdigest()
            return '{}-{}'.format(etag_csum, len(self._etag_parts))
        else:
            return self._etag_hasher.hexdigest()


class ChecksummingBufferedReader:

    def __init__(self, *args, **kwargs):
        self._hashers = dict(crc32c=crcmod.predefined.Crc("crc-32c"),
                             sha1=hashlib.sha1(),
                             sha256=hashlib.sha256(),
                             s3_etag=S3Etag())
        self._reader = BufferedReader(*args, **kwargs)
        self.raw = self._reader.raw

    def read(self, size=None):
        chunk = self._reader.read(size)
        if chunk:
            for hasher in self._hashers.values():
                hasher.update(chunk)
        return chunk

    def get_checksums(self):
        checksums = {}
        checksums.update({name: hasher.hexdigest() for name, hasher in self._hashers.items()})
        return checksums

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass


class File(object):
    def __init__(self, name=None, bundle=None, size=None, content_type=None, origin_url=None):
        self.name = name
        self.bundle = bundle
        self.uuid = str(uuid.uuid4())
        self.size = size or os.stat(self.path()).st_size
        self.content_type = content_type
        self.origin_url = origin_url
        self.staged_url = None
        self.version = None
        self.checksums = {}

    def __eq__(self, other):
        return self.bundle == other.bundle and self.name == other.name

    def is_metadata(self):
        return self.content_type == 'application/json'

    def path(self):
        return os.path.join(self.bundle.path, self.name)


class MetadataFile(File):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.content_type = self.content_type or 'application/json'


class DataFile(File):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.content_type = self.content_type or 'application/octet-stream'


class Bundle(object):

    def __init__(self, path=None):
        self.path = path
        self.files = dict()  # 'file name': File

    def add_file(self, file):
        file.bundle = self
        self.files[file.name] = file


class LocalBundle(Bundle):

    MANIFEST_FILENAME = 'manifest.json'
    BUNDLE_HOME_DIRNAME = 'bundles'  # Folders containing bundles

    def __init__(self, local_path):
        super(self.__class__, self).__init__(path=local_path)
        self.uuid = str(uuid.uuid4())
        self.enumerate_local_files()

    def enumerate_local_files(self):
        for path in glob.glob(self.path + '/*'):
            name = os.path.basename(path)
            if re.search(".json$", name):
                self.add_file(MetadataFile(name=name, bundle=self))
            else:
                self.add_file(DataFile(name=name, bundle=self))


class DSSAPIError(RuntimeError):
    pass


class DSSDriver(object):

    FAKE_CREATOR_UID = 104
    DEFAULT_DSS_REPLICA = 'aws'
    BACKOFF_FACTOR = 1.618
    RESPONSE_FIELDS_TO_DUMP = ('status_code', 'reason', 'content', 'headers', 'url', 'history', 'encoding', 'elapsed')

    def __init__(self, endpoint_url, report_task_ids=False):
        self.dss_url = endpoint_url
        self.report_task_ids = report_task_ids

    def put_file(self, bundle_uuid, file_uuid, file_location):
        raise NotImplementedError()

    def put_bundle(self, bundle_uuid, file_info):
        raise NotImplementedError()

    def _dump_response(self, response):
        return "\n".join(["\t%s=%s" % (attr, getattr(response, attr)) for attr in self.RESPONSE_FIELDS_TO_DUMP])


class DSSrestDriver(DSSDriver):

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)

    def put_file(self, bundle_uuid, file_uuid, file_location):
        payload = {
            'bundle_uuid': bundle_uuid,
            'creator_uid': 104,
            'source_url': file_location,
        }
        url = "%s/files/%s" % (self.dss_url, file_uuid)
        params = {'version': datetime.now().isoformat()}
        response = requests.put(url, params=params, json=payload)
        if response.status_code == 201:
            return response.json()['version']
        elif response.status_code == 202:
            if self.report_task_ids:
                logger.info("\n    ACCEPTED: task_id=%s, waiting." % (response.json()['task_id'],))
            response = self._wait_for_file_to_exist(file_uuid)
            return response.headers['X-DSS-VERSION']
        else:
            raise DSSAPIError("put(%s, %s, %s) returned:\n%s" %
                              (url, params, payload, self._dump_response(response)))

    def head_file(self, file_uuid, version=None):
        if version:
            url = self.dss_url + "/files/" + file_uuid + "?version=" + version
        else:
            url = self.dss_url + "/files/" + file_uuid
        params = {'replica': self.DEFAULT_DSS_REPLICA}
        response = requests.head(url, params=params)
        return response

    def put_bundle(self, bundle_uuid, file_info):
        payload = {
            'creator_uid': self.FAKE_CREATOR_UID,
            'files': file_info
        }
        url = self.dss_url + "/bundles/" + bundle_uuid
        params = {'version': datetime.now().isoformat(), 'replica': 'aws'}
        response = requests.put(url, params=params, json=payload)
        if response.status_code != 201:
            raise DSSAPIError("put(%s, %s, %s) returned:\n%s" %
                              (url, params, payload, self._dump_response(response)))
        return response.json()['version']

    def _wait_for_file_to_exist(self, file_uuid, timeout_seconds=30*60):
        timeout = time.time() + timeout_seconds
        wait = 1.0
        while time.time() < timeout:
            response = self.head_file(file_uuid)
            if response.status_code == 200:
                return response
            elif response.status_code in (404, 504):
                time.sleep(wait)
                progress(".")
                wait = min(60.0, wait * self.BACKOFF_FACTOR)
            else:
                raise RuntimeError(response)
        else:
          raise RuntimeError("File %s did not appear within %d seconds" % (file_uuid, timeout_seconds))


class DataStoreAPI:

    DEFAULT_DSS_URL_TEMPLATE = "https://dss.%s.data.humancellatlas.org/v1"

    def __init__(self, deployment, driver='rest', report_task_ids=False):
        driver_name = 'DSS' + driver + 'Driver'
        dss_api_url = self.DEFAULT_DSS_URL_TEMPLATE % (deployment,)
        self.driver = eval(driver_name)(endpoint_url=dss_api_url, report_task_ids=report_task_ids)

    def put_file(self, *args, **kwargs):
        return self.driver.put_file(*args, **kwargs)

    def put_bundle(self, *args, **kwargs):
        return self.driver.put_bundle(*args, **kwargs)


class S3Location(DotMap):

    def __init__(self, url):
        urlbits = parse_url(url)
        if urlbits.scheme != 's3':
            raise RuntimeError("Not an S3 URL!")
        super(self.__class__, self).__init__(Bucket=urlbits.netloc, Key=urlbits.path.lstrip('/'))

    def __str__(self):
        return "s3://%s/%s" % (self.Bucket, self.Key)


class S3Agent:

    CLEAR_TO_EOL = "\x1b[0K"

    def __init__(self, credentials={}):
        session = boto3.session.Session(**credentials)
        self.s3 = session.resource('s3')
        self.s3client = session.client('s3')

    def _file_upload_progress_callback(self, bytes_transferred):
        self.cumulative_bytes_transferred += bytes_transferred
        percent_complete = (self.cumulative_bytes_transferred * 100) / self.file_being_transferred_size
        duration = time.time() - self.file_upload_start_time
        rate_mb_s = (self.cumulative_bytes_transferred / duration) / MB
        progress("\rUploading %s: %s of %s transferred [%.0f%%] (%.1f MiB/s) %s" %
                 (self.file_being_transferred_name,
                  sizeof_fmt(self.cumulative_bytes_transferred),
                  sizeof_fmt(self.file_being_transferred_size),
                  percent_complete,
                  rate_mb_s,
                  self.CLEAR_TO_EOL))

    def upload_and_checksum(self, local_path, target_bucket, target_key, content_type, file_size):
        self.file_being_transferred_name = os.path.basename(local_path)
        self.file_being_transferred_size = file_size
        self.file_upload_start_time = time.time()
        progress("Uploading %s: " % (self.file_being_transferred_name,))
        bucket = self.s3.Bucket(target_bucket)
        with io.open(local_path, 'rb') as fh:
            self.cumulative_bytes_transferred = 0
            reader = ChecksummingBufferedReader(fh)
            obj = bucket.Object(target_key)
            obj.upload_fileobj(reader,
                               ExtraArgs={
                                   'ContentType': content_type,
                                   'ACL': 'bucket-owner-full-control'
                               },
                               Callback=self._file_upload_progress_callback,
                               Config=self.transfer_config(file_size)
                               )
        sys.stdout.write("\n")
        return reader.get_checksums()

    def add_tagging(self, s3url, tags):
        s3loc = S3Location(s3url)
        tagging = dict(TagSet=self._encode_tags(tags))
        self.s3client.put_object_tagging(Bucket=s3loc.Bucket, Key=s3loc.Key, Tagging=tagging)

    @staticmethod
    def _encode_tags(tags):
        return [dict(Key=k, Value=v) for k, v in tags.items()]

    @classmethod
    def transfer_config(cls, file_size):
        etag_stride = cls._s3_chunk_size(file_size)
        return TransferConfig(multipart_threshold=etag_stride,
                              multipart_chunksize=etag_stride)

    @staticmethod
    def _s3_chunk_size(file_size):
        if file_size <= 10000 * 64 * MB:
            return 64 * MB
        else:
            div = file_size // 10000
            if div * 10000 < file_size:
                div += 1
            return ((div + (MB-1)) // MB) * MB


class S3ObjectTagger:

    MIME_TAG = 'hca-dss-content-type'

    def __init__(self, s3agent, target_url):
        self.s3 = s3agent
        self.target_url = target_url

    def tag_using_these_checksums(self, raw_sums):
        tags = self._hca_checksum_tags(raw_sums)
        tags.update(self._generate_mime_tags())
        self.s3.add_tagging(self.target_url, tags)

    def _generate_mime_tags(self):
        mime_type = mimetypes.guess_type(S3Location(self.target_url).Key)[0]
        if mime_type is None:
            mime_type = "application/octet-stream"
        return {self.MIME_TAG: mime_type}

    @staticmethod
    def _hca_checksum_tags(checksums):
        return {
            'hca-dss-s3_etag': checksums['s3_etag'],
            'hca-dss-sha1':    checksums['sha1'],
            'hca-dss-sha256':  checksums['sha256'],
            'hca-dss-crc32c':  checksums['crc32c'],
        }


class StagingArea:

    STAGING_API_URL_TEMPLATE = 'https://staging.%s.data.humancellatlas.org/v1/'
    STAGING_SERVICE_API_KEY = os.environ['STAGING_SERVICE_API_KEY']
    CONTENT_TYPES = {
        DataFile: 'binary/octet-stream',
        MetadataFile: 'application/json',
    }

    def __init__(self, deployment='dev'):
        self.staging_api_url = self.STAGING_API_URL_TEMPLATE % (deployment,)
        self.bucket_name = 'org-humancellatlas-staging-' + deployment
        self.uuid = str(uuid.uuid4())
        self._create_area()

    def _create_area(self):
        progress("Creating staging area...")
        response = self._api_call('post', 'area/' + self.uuid, expected_response_status=201)
        junk, junk, junk, junk, junk, encoded_credentials = response.json()['urn'].split(':')
        uppercase_credentials = json.loads(base64.b64decode(encoded_credentials))
        aws_credentials = {k.lower(): v for k, v in uppercase_credentials.items()}
        self.s3 = S3Agent(credentials=aws_credentials)
        time.sleep(10)
        print("s3://%s/%s" % (self.bucket_name, self.uuid))

    def delete(self):
        progress("Deleting staging area... ")
        self._api_call('delete', "area/" + self.uuid, expected_response_status=204)
        sys.stdout.write("done.\n")

    def stage_file(self, file):
        progress("Uploading %s: " % (file.name,))
        file_s3_key = "%s/%s" % (self.uuid, file.name)
        content_type = self.CONTENT_TYPES[file.__class__]
        checksums = self.s3.upload_and_checksum(file.path(), self.bucket_name, file_s3_key, content_type, file.size)
        file.staged_url = "s3://%s/%s" % (self.bucket_name, file_s3_key)
        S3ObjectTagger(self.s3, file.staged_url).tag_using_these_checksums(checksums)

    def _api_call(self, method, path, params={}, headers={}, expected_response_status=200):
        url = self.staging_api_url + path
        all_headers = dict(headers)
        all_headers['Api-Key'] = self.STAGING_SERVICE_API_KEY
        verb_method = getattr(requests, method)
        response = verb_method(url, params=params, headers=all_headers)
        if response.status_code != expected_response_status:
            raise RuntimeError("Unexpected response to %s %s: %d %s" %
                               (method, url, response.status_code, response.content))
        return response


class Main:

    def __init__(self):
        self._parse_args()
        self.staging_area = StagingArea(deployment=self.args.deployment)
        self.dss = DataStoreAPI(self.args.deployment)
        self._stage_and_store_bundle(self.args.bundle_path)

    def _stage_and_store_bundle(self, bundle_path):
        bundle = LocalBundle(bundle_path)
        for file in bundle.files.values():
            self.staging_area.stage_file(file)
        for file in bundle.files.values():
            self._store_file(bundle, file)
        self._register_bundle(bundle)
        self.staging_area.delete()
        print("You may view your bundle at %s/bundles/%s" % (self.dss.driver.dss_url, bundle.uuid))

    def _parse_args(self):
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('bundle_path', metavar="<path-to-bundle>",
                            help="Path to folder containing metadata/data")
        parser.add_argument('-d', '--deployment', default='dev',
                            help="Deployment environment (default=dev)")
        self.args = parser.parse_args()

    def _store_file(self, bundle, file):
        progress("Storing %s as %s..." % (file.name, file.uuid))
        start_time = time.time()
        file.version = self.dss.put_file(bundle.uuid, file.uuid, file.staged_url)
        duration = time.time() - start_time
        rate_mb_s = file.size / duration / MB
        print("%s (%.1f MiB/s)" % (file.version, rate_mb_s))

    def _register_bundle(self, bundle):
        progress("Registering bundle %s... " % (bundle.uuid,))
        file_info = []
        for file in bundle.files.values():
            file_info.append({
                'name': file.name,
                'uuid': file.uuid,
                'version': file.version,
                'indexed': file.is_metadata()
            })
        version = self.dss.put_bundle(bundle.uuid, file_info)
        print(version)


if __name__ == '__main__':
    runner = Main()
