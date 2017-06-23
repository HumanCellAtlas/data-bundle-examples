import uuid
from functools import reduce
import boto3


class S3ExampleFile:
    def __init__(self, object_summary, bundle):
        self.s3_object_summary = object_summary
        self.bundle = bundle
        self.path = object_summary.key
        self.url = f"s3://{bundle.bucket_str}/{self.path}"
        self.timestamp = object_summary.last_modified
        self.uuid = str(uuid.uuid4())

    def s3object(self):
        return self.bundle.bucket.Object(self.path)

    def get_tagging(self):
        s3client = self.bundle.s3.meta.client
        response = s3client.get_object_tagging(Bucket=self.s3_object_summary.bucket_name,
                                               Key=self.s3_object_summary.key)
        return self._decode_tags(response['TagSet'])

    def add_tagging(self, tags: dict):
        s3client = self.bundle.s3.meta.client
        tagging = dict(TagSet=self._encode_tags(tags))
        print("TAGS: "+str(tagging))
        s3client.put_object_tagging(Bucket=self.s3_object_summary.bucket_name,
                                    Key=self.s3_object_summary.key,
                                    Tagging=tagging)
        #print(str(s3client.get_object_tagging(Bucket=self.s3_object_summary.bucket_name, Key=self.s3_object_summary.key)))

    # tags = [{'Key': 'a', 'Value': '1'}, {'Key': 'b', 'Value': '2'}]
    # simplified_dicts = [{'a': '1'}, {'b': '2'}]
    # returns {'a': '1', 'b': '2'}
    @staticmethod
    def _decode_tags(tags):
        if not tags:
            return {}
        simplified_dicts = list({tag['Key']: tag['Value']} for tag in tags)
        return reduce(lambda x, y: dict(x, **y), simplified_dicts)

    @staticmethod
    def _encode_tags(tags):
        return [dict(Key=k, Value=v) for k, v in tags.items()]
