#!/usr/bin/env python3.6

import mimetypes
from checksumming_io.checksumming_io import ChecksummingSink
from s3_examples import S3ExampleBundle, S3ExampleFile


class AddS3HcaTags:

    CHECKSUM_TAGS = ['hca-dss-sha1', 'hca-dss-sha256', 'hca-dss-crc32c', 'hca-dss-s3_etag']
    MIME_TAG = 'hca-dss-content-type'

    def __init__(self):
        self.run()

    def run(self):
        for bundle in S3ExampleBundle.all():
            print("Bundle: ", bundle.path)
            self.add_tagging_for_bundle(bundle)

    def add_tagging_for_bundle(self, bundle: S3ExampleBundle):
        for file in bundle.files:
            print("    File: ", file.path)
            self.add_tagging_for_file(file)

    def add_tagging_for_file(self, file: S3ExampleFile):
        current_tags = file.get_tagging()
        new_tags = {}
        new_tags.update(self.additional_checksum_tags(file, current_tags))
        new_tags.update(self.additional_mime_tags(file, current_tags))
        if new_tags:
            print("        Adding tags: ", list(new_tags.keys()))
            all_tags = dict(current_tags, **new_tags)
            file.add_tagging(all_tags)

    def additional_checksum_tags(self, file: S3ExampleFile, current_tags: dict):
        if self.checksum_tags_are_all_present(current_tags):
            return {}
        else:
            sums = self.compute_checksums(file)
            return {
                'hca-dss-s3_etag': sums['s3_etag'],
                'hca-dss-sha1':    sums['sha1'],
                'hca-dss-sha256':  sums['sha256'],
                'hca-dss-crc32c':  sums['crc32c'],
            }

    def additional_mime_tags(self, file: S3ExampleFile, current_tags: dict):
        if 'content-type' in current_tags:
            return {}
        else:
            return {'content-type': mimetypes.guess_type(file.path)[0]}

    def checksum_tags_are_all_present(self, actual_tags: dict) -> bool:
        # TODO try python has map(), filter(), reduce(), all()
        tags_present = [tag for tag in self.CHECKSUM_TAGS if tag in actual_tags.keys()]
        return len(tags_present) == len(self.CHECKSUM_TAGS)

    @staticmethod
    def compute_checksums(file: S3ExampleFile) -> dict:
        with ChecksummingSink() as sink:
            file.s3object().download_fileobj(sink)
            return sink.get_checksums()

if __name__ == '__main__':
    AddS3HcaTags()
