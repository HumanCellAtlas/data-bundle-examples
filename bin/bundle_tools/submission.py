import copy, json, re

from botocore.exceptions import ClientError

from .s3 import S3Agent
import bundle_tools


class SubmissionInfo:

    SUBMISSION_FILENAME = "submission.json"

    def __init__(self, bucket_name, bundle):
        self.bucket_name = bucket_name
        self.bundle = bundle
        self.info = None
        self.orig_info = None
        self.s3_obj = S3Agent().object(f"s3://{self.bucket_name}/{self.bundle.path}/{self.SUBMISSION_FILENAME}")

    def load(self):
        try:
            submission_json = self.s3_obj.get()['Body'].read()
            self.info = json.loads(submission_json)
            self.orig_info = copy.deepcopy(self.info)
            for file in self.export_files():
                self.bundle.add_file(file)
        except ClientError:
            self.info = {}

    def save(self) -> bool:
        self.extract_bundle_info()
        if self.info != self.orig_info:
            self.s3_obj.put(Body=json.dumps(self.info))
            return True
        else:
            return False

    def export_files(self):
        return map(self._convert_file_entry_to_file, self.info['files'])

    def extract_bundle_info(self):
        if hasattr(self.bundle, 'uuid'):
                self.info['bundle_uuid'] = self.bundle.uuid
        file_entries = self.info.setdefault('files', [])
        for file in self.bundle.files.values():
            try:
                file_entry = next((item for item in file_entries if item["name"] == file.name))
            except StopIteration:
                file_entry = dict()
                file_entries.append(file_entry)
            self.update_file_entry(file, file_entry)

    def update_file_entry(self, file, entry: dict):
        for attr in ['name', 'content_type', 'size', 'staged_url', 'origin_url', 'uuid']:
            if hasattr(file, attr):
                if getattr(file, attr):
                    entry[attr] = getattr(file, attr)

    def _convert_file_entry_to_file(self, file_entry):
        if re.match(".*\.json", file_entry['name']):
            return bundle_tools.MetadataFile(**file_entry)
        else:
            return bundle_tools.DataFile(**file_entry)

