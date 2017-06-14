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
from urllib.request import urlopen, urlretrieve
#from urllib2 import urlopen, Request
from subprocess import Popen, PIPE


class GetImportData:
    def __init__(self):
        parser = argparse.ArgumentParser(description='Downloads data files for the various bundles.')
        parser.add_argument('--input-dir', default='.', required=True)

        # get args
        args = parser.parse_args()
        self.input_dir = args.input_dir

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
            print ("Downloading: "+dir+"/"+name)
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            urlretrieve(str(dir+"/"+name), directory+"/"+name)


# run the class
if __name__ == '__main__':
    runner = GetImportData()
