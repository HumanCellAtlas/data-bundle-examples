import uuid
import boto3
from .s3_example_file import S3ExampleFile


class S3ExampleBundle:

    BUNDLE_EXAMPLES_BUCKET = 'hca-dss-test-src'
    BUNDLE_EXAMPLES_ROOT = 'data-bundle-examples'
    BUNDLE_EXAMPLES_BUNDLE_LIST_PATH = f"{BUNDLE_EXAMPLES_ROOT}/import/bundle_list"

    s3 = boto3.resource('s3')
    _examples_s3_bucket = s3.Bucket(BUNDLE_EXAMPLES_BUCKET)

    def __init__(self, bundle_path):
        self.bucket = self._examples_s3_bucket
        self.path = bundle_path
        self.uuid = str(uuid.uuid4())
        self.files = self._get_s3_files()

    @classmethod
    def all(cls):
        bundle_list_s3object = cls._examples_s3_bucket.Object(cls.BUNDLE_EXAMPLES_BUNDLE_LIST_PATH)
        bundle_list = bundle_list_s3object.get()['Body'].read().decode('utf-8').split("\n")
        for bundle_path in bundle_list:
            yield cls(bundle_path)

    def _get_s3_files(self):
        bundle_folder_path = f"{self.BUNDLE_EXAMPLES_ROOT}/{self.path}"
        object_summaries = self._examples_s3_bucket.objects.filter(Prefix=bundle_folder_path)
        return [S3ExampleFile(objectSummary, self) for objectSummary in object_summaries]
