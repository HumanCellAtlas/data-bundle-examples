import uuid
import boto3
from .s3_example_file import S3ExampleFile


class S3ExampleBundle:

    #BUNDLE_EXAMPLES_BUCKET = 'hca-dss-test-src'
    #BUNDLE_EXAMPLES_ROOT = 'data-bundles-examples'
    BUNDLE_EXAMPLES_BUNDLE_LIST_PATH = "import/bundle_list"

    def __init__(self, bucket, root, bundle_path):
        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(bucket)
        self.bucket_str = bucket
        self.root = root
        self.path = bundle_path
        self.uuid = str(uuid.uuid4())
        self.files = self._get_s3_files()

    @classmethod
    def some(cls, bucket, root, bundle_path):
        yield cls(bucket, root, bundle_path)

    @classmethod
    def all(cls, bucket, root):
        bundle_list_s3object = cls.bucket.Object(root+"/"+cls.BUNDLE_EXAMPLES_BUNDLE_LIST_PATH)
        bundle_list = bundle_list_s3object.get()['Body'].read().decode('utf-8').split("\n")
        for bundle_path in bundle_list:
            yield cls(bundle_path, bucket, root)

    def _get_s3_files(self):
        bundle_folder_path = f"{self.root}/{self.path}/"
        object_summaries = self.bucket.objects.filter(Prefix=bundle_folder_path)
        return [S3ExampleFile(objectSummary, self) for objectSummary in object_summaries]
