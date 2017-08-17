import mimetypes
from functools import reduce
from dotmap import DotMap
from urllib.parse import urlparse
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from checksumming_io.checksumming_io import ChecksummingBufferedReader, ChecksummingSink, S3Etag
from .parallel_logger import logger


KB = 1024
MB = KB * KB


class S3Location(DotMap):

    def __init__(self, url):
        urlbits = urlparse(url)
        if urlbits.scheme != 's3':
            raise RuntimeError("Not an S3 URL!")
        super().__init__(Bucket=urlbits.netloc, Key=urlbits.path.lstrip('/'))

    def __str__(self):
        return f"s3://{self.Bucket}/{self.Key}"


class S3Agent:

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.s3client = self.s3.meta.client

    def copy_between_buckets(self, src_url: str, dest_url: str, file_size: int):
        src = S3Location(src_url)
        dest = S3Location(dest_url)
        obj = self.s3.Bucket(dest.Bucket).Object(dest.Key)
        obj.copy(src.toDict(),
                 Config=self._transfer_config(file_size),
                 ExtraArgs={'ACL': 'bucket-owner-full-control'})

    def upload_and_checksum(self, local_path: str, target_url: str, file_size: int) -> dict:
        target = S3Location(target_url)
        bucket = self.s3.Bucket(target.Bucket)
        with open(local_path, 'rb') as fh:
            reader = ChecksummingBufferedReader(fh)
            obj = bucket.Object(target.Key)
            obj.upload_fileobj(reader,
                               Config=self._transfer_config(file_size),
                               ExtraArgs={'ACL': 'bucket-owner-full-control'})
        return reader.get_checksums()

    def copy_object_tagging(self, src_url: str, dest_url: str):
        tags = self.get_tagging(src_url)
        self.add_tagging(dest_url, tags)

    def get_tagging(self, s3url: str) -> dict:
        s3loc = S3Location(s3url)
        response = self.s3client.get_object_tagging(Bucket=s3loc.Bucket, Key=s3loc.Key)
        return self._decode_tags(response['TagSet'])

    def add_tagging(self, s3url: str, tags: dict):
        s3loc = S3Location(s3url)
        tagging = dict(TagSet=self._encode_tags(tags))
        self.s3client.put_object_tagging(Bucket=s3loc.Bucket, Key=s3loc.Key, Tagging=tagging)

    def get_object(self, s3url: str):
        try:
            obj = self.object(s3url)
            obj.load()
            return obj
        except ClientError:
            return None

    def object(self, s3url: str):
        s3loc = S3Location(s3url)
        return self.s3.Object(s3loc.Bucket, s3loc.Key)

    def delete_object(self, s3url: str):
        s3loc = S3Location(s3url)
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

    @classmethod
    def _transfer_config(cls, file_size: int) -> TransferConfig:
        etag_stride = cls._s3_chunk_size(file_size)
        return TransferConfig(multipart_threshold=etag_stride,
                              multipart_chunksize=etag_stride)

    @staticmethod
    def _s3_chunk_size(file_size: int) -> int:
        if file_size <= 10000 * 64 * MB:
            return 64 * MB
        else:
            div = file_size // 10000
            if div * 10000 < file_size:
                div += 1
            return ((div + (MB-1)) // MB) * MB


class S3ObjectTagger:
    """
    Tag existing S3 objects
    """

    CHECKSUM_TAGS = ('hca-dss-sha1', 'hca-dss-sha256', 'hca-dss-crc32c', 'hca-dss-s3_etag')
    MIME_TAG = 'hca-dss-content-type'
    ALL_TAGS = CHECKSUM_TAGS + (MIME_TAG,)

    def __init__(self, target: str):
        self.target = target
        self.s3 = S3Agent()

    def copy_tags_from_object(self, s3url: str):
        self.s3.copy_object_tagging(s3url, self.target)
        self.complete_tags()

    def tag_using_these_checksums(self, raw_sums: dict):
        tags = self._hca_checksum_tags(raw_sums)
        tags.update(self._generate_mime_tags())
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
        sums = self._compute_checksums_from_s3(str(self.target))
        return self._hca_checksum_tags(sums)

    def _generate_mime_tags(self) -> dict:
        mime_type = mimetypes.guess_type(S3Location(self.target).Key)[0]
        if mime_type is None:
            mime_type = "application/octet-stream"
        return {self.MIME_TAG: mime_type}

    def _compute_checksums_from_s3(self, s3url: str) -> dict:
        s3loc = S3Location(s3url)
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
