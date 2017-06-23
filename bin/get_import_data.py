#!/usr/bin/env python

"""
    author Brian O'Connor
    broconno@ucsc.edu
    This module first crawsl the filesystem looking for manifest.json files, parses
    them, finds data to download, and downloads them.
    Example: python bin/get_import_data.py --input-dir import --output-s3-dir s3://hca-dss-test-src/data-bundle-examples --tmp-dir /tmp --test
    For where Clay was working (note plural "bundles"):
    python bin/get_import_data.py --input-dir import --output-s3-dir s3://hca-dss-test-src/data-bundles-examples --tmp-dir /tmp --test
    Tested with Python 3.6.0
"""

import os
import os.path
import platform
import argparse
import json
import jsonschema
import datetime
import re
import dateutil
import ssl
import dateutil.parser
import ast
import sys
import boto
from boto.s3.key import Key
from urllib.request import urlopen, urlretrieve
#from urllib2 import urlopen, Request
from subprocess import Popen, PIPE
import mimetypes
from checksumming_io.checksumming_io import ChecksummingSink
from s3_examples import S3ExampleBundle, S3ExampleFile


class GetImportData:

    CHECKSUM_TAGS = ['hca-dss-sha1', 'hca-dss-sha256', 'hca-dss-crc32c', 'hca-dss-s3_etag']
    MIME_TAG = 'hca-dss-content-type'

    def __init__(self):
        parser = argparse.ArgumentParser(description='Downloads data files for the various bundles.')
        parser.add_argument('--input-dir', required=True)
        parser.add_argument('--output-s3-dir', required=True)
        parser.add_argument('--test', action='store_true', default=False)
        parser.add_argument('--cleanup', action='store_true', default=False)
        parser.add_argument('--tmp-dir', required=True)

        # get args
        args = parser.parse_args()
        self.input_dir = args.input_dir
        self.output_s3_dir = args.output_s3_dir
        self.conn = boto.connect_s3()
        self.test = args.test
        self.cleanup = args.cleanup
        self.tmp_dir = args.tmp_dir
        # tracking the number of files that are missing from S3
        self.missing_files = 0

        # run
        self.run()


    def run(self):
        # walk directory structure, parse JSONs, download data files, upload to S3 while tagging
        for root, dirs, files in os.walk(self.input_dir):
            for file in files:
                if file == "manifest.json":
                    print(root+"/manifest.json")
                    manifest_file = open(root+"/manifest.json", "r")
                    manifest_struct = json.loads(manifest_file.read())
                    self.download(manifest_struct, root)
                    manifest_file.close()

    def download(self, struct, directory):
        dir = struct['dir']
        for file in struct['files']:
            name = file['name']
            #print ("Downloading: "+dir+"/"+name)
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            try:
                if (self.source_newer_or_diff_size(str(dir+"/"+name), self.output_s3_dir+"/"+directory+"/"+name)):
                    print("DOWNLOADING: "+str(dir+"/"+name)+" TO: "+directory+"/"+name)
                    if (self.test):
                        print("TESTING WON'T DOWNLOAD")
                    else:
                        urlretrieve(str(dir+"/"+name), directory+"/"+name)
                        self.upload(directory+"/"+name)
                        if (self.cleanup):
                            os.remove(directory+"/"+name)
                else:
                    print("SKIPPING DOWNLOAD: "+str(dir+"/"+name)+" TO: "+directory+"/"+name+" FILE SIZES IDENTICAL")
            except Exception as error:
                print ("ERROR: "+error)
                # just exit with non-zero status
                sys.exit(1)
        # now try tagging, this needs to be reworked so tagging happens *before* upload to avoid double-download!!


    def upload(self, path):
        print("UPLOADING: "+path+" to "+self.output_s3_dir+"/"+path)
        (bucket, key) = self.parse_bucket_key(self.output_s3_dir+"/"+path)
        file = open(path, 'rb')
        if (self.test):
            print("TESTING WON'T UPLOAD")
        elif (self.upload_to_s3(file, bucket, key)):
            print("FINISHED!")
        else:
            print("FAILED TO UPLOAD")

    def source_newer_or_diff_size(self, web_source, s3_destination):
        site = urlopen(web_source)
        meta = site.info()
        #print (meta)
        web_size = int(meta.get("Content-Length"))
        print ("Content-Length:", meta.get("Content-Length"))
        (s3_bucket, s3_key) = self.parse_bucket_key(s3_destination)
        print ("S3Bucket: "+s3_bucket+" S3Key: "+s3_key)
        bk = self.conn.get_bucket(s3_bucket)
        key = bk.lookup(s3_key)
        if (key == None):
            print ("S3 key doesn't exist")
            return(True)
        else:
            print ("S3 Size: "+str(key.size))
        s3_size = int(key.size)
        return(s3_size != web_size)

    def parse_bucket_key(self, s3_path):
        m = re.search('^s3://([^/]+)/(\S+)$', s3_path)
        s3_bucket = m.group(1)
        s3_key = m.group(2)
        return(s3_bucket, s3_key)

    def upload_to_s3(self, file, bucket, key, callback=None, md5=None, reduced_redundancy=False, content_type=None):
        """
        see http://stackabuse.com/example-upload-a-file-to-aws-s3/
        Uploads the given file to the AWS S3
        bucket and key specified.

        callback is a function of the form:

        def callback(complete, total)

        The callback should accept two integer parameters,
        the first representing the number of bytes that
        have been successfully transmitted to S3 and the
        second representing the size of the to be transmitted
        object.

        Returns boolean indicating success/failure of upload.
        """
        try:
            size = os.fstat(file.fileno()).st_size
        except:
            # Not all file objects implement fileno(),
            # so we fall back on this
            file.seek(0, os.SEEK_END)
            size = file.tell()

        bucket_obj = self.conn.get_bucket(bucket, validate=True)
        k = Key(bucket_obj)
        k.key = key
        print ("UPLOADING TO BUCKET: "+bucket+" KEY: "+key)
        if content_type:
            k.set_metadata('Content-Type', content_type)
        sent = k.set_contents_from_file(file, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)

        # Rewind for later use
        file.seek(0)

        if sent == size:
            # can I add the call here?
            m = re.search('^data-bundles-examples/(\S+)/[^/+]$', key)
            print ("THE BUNDLE LOCATION: "+m.group(1))
            for bundle in S3ExampleBundle.some(m.group(1)):
                print("Bundle: ", bundle.path)
                print(type(bundle))
                self.add_tagging_for_bundle(bundle)
                sys.exit(1)
            return True
        return False

    def run_some(self, bundle_path):
        for bundle in S3ExampleBundle.some(bundle_path):
            print("Bundle: ", bundle.path)
            print(type(bundle))
            self.add_tagging_for_bundle(bundle)

    def add_tagging_for_bundle(self, bundle: S3ExampleBundle):
        for file in bundle.files:
            print("    File: ", file.path)
            print(type(file))
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

# run the class
if __name__ == '__main__':
    runner = GetImportData()
