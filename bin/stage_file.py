#!/usr/bin/env python3.6

"""
Stage files in the HCA Staging Area

Commands:

    stage_file cp <file> <urn>

"""

import argparse, base64, json, os

from bundle_tools.s3 import S3Agent


class Main:

    STAGING_BUCKET = "org-humancellatlas-staging-dev"

    def __init__(self):
        self._parse_args()
        junk, junk, junk, self.area_uuid, encoded_credentials = self.args.urn.split(':')
        uppercase_credentials = json.loads(base64.b64decode(encoded_credentials))
        self.credentials = {k.lower(): v for k, v in uppercase_credentials.items()}
        self.s3 = S3Agent(credentials=self.credentials)
        self._stage_file(self.args.file_path)

    def _parse_args(self):
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('file_path', metavar="<file>",
                            help="name of file to stage")
        parser.add_argument('urn', metavar='<URN>',
                            help="URN of staging area (given to you by Ingest Broker)")
        self.args = parser.parse_args()

    def _stage_file(self, file_path):
        target_url = f"s3://{self.STAGING_BUCKET}/{self.area_uuid}/{file_path}"
        file_size = os.stat(file_path).st_size
        checksums = self.s3.upload_and_checksum(self.args.file_path, target_url, file_size)
        tags = {
            'hca-dss-content-type': 'hca-data-file',
            'hca-dss-s3_etag': checksums['s3_etag'],
            'hca-dss-sha1': checksums['sha1'],
            'hca-dss-sha256': checksums['sha256'],
            'hca-dss-crc32c': checksums['crc32c'],
        }
        self.s3.add_tagging(target_url, tags)


if __name__ == '__main__':
    runner = Main()
