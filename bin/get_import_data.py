#!/usr/bin/env python

"""
    author Brian O'Connor
    broconno@ucsc.edu
    This module first crawsl the filesystem looking for manifest.json files, parses
    them, finds data to download, and downloads them.
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
from urllib.request import urlopen, urlretrieve
#from urllib2 import urlopen, Request
from subprocess import Popen, PIPE


class GetImportData:
    def __init__(self):
        parser = argparse.ArgumentParser(description='Downloads data files for the various bundles.')
        parser.add_argument('--input-dir', default='.', required=True)
        parser.add_argument('--output-s3-dir', default='s3://hca-dss-test-src/data-bundle-examples/', required=True)

        # get args
        args = parser.parse_args()
        self.input_dir = args.input_dir
        self.output_s3_dir = args.output_s3_dir

        # run
        self.run()

    def run(self):
        # walk directory structure, parse JSONs, put in single json, write ES index file
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
                print("DOWNLOADING: "+str(dir+"/"+name)+" TO: "+directory+"/"+name)
                if (self.source_newer_or_diff_size(str(dir+"/"+name), self.output_s3_dir+"/"+directory+"/"+name)):
                    #urlretrieve(str(dir+"/"+name), directory+"/"+name)
                    self.upload(directory+"/"+name)
            except Exception as error:
                print ("ERROR: "+error)
                # just exit with non-zero status
                sys.exit(1)

    def upload(self, path):
        print("UPLOADING: "+path+" to "+self.output_s3_dir+"/"+path)

    def source_newer_or_diff_size(self, web_source, s3_destination):
        site = urlopen(web_source)
        meta = site.info()
        print (meta)
        web_size = meta.get("Content-Length")
        print ("Content-Length:", meta.get("Content-Length"))
        conn = boto.connect_s3()
        m = re.search('^s3://([^/]+)/(\S+)$', s3_destination)
        s3_bucket = m.group(1)
        s3_key = m.group(2)
        print ("S3Bucket: "+s3_bucket+" S3Key: "+s3_key)
        bk = conn.get_bucket(s3_bucket)
        key = bk.lookup(s3_key)
        if (key == None):
            print ("S3 key doesn't exist")
            return(True)
        else:
            print ("S3 Size: "+key.size)
        s3_size = key.size
        return(s3_size != web_size)

# run the class
if __name__ == '__main__':
    runner = GetImportData()
