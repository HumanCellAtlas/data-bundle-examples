#!/usr/bin/env python3.6

import argparse, signal, ssl, sys
from concurrent.futures import ProcessPoolExecutor
from bundle_tools import LocalBundle, BundleStager
from bundle_tools import logger

"""
    stager.py - Stage Example Data Bundles in S3 Bucket org-humancellatlas-data-bundle-examples
    
    Un-tar metadata files before running: tar xf import/import.tgz
    
    Default action is to traverse the import/ folder finding bundles, then for each bundle:
        For each data file:
            - Find the original (using manifest.json) and note its size.
            - If this version is not at the desired location, download it, then upload it to S3.
        For each metadata file:
            - Checksum it.
            - Upload to S3 unless a files already exists there with this checksum.
            
    Checking 100,000 files can be a slow process, so you can parallelize with the -j option.
    Try running on an m4.2xlarge with -j16.  This will take under an hour and works well in
    the case where there are no new data-files to be uploaded.  Note however that if there
    are new data-files to be uploaded, you will want to use minimal or no concurrency for
    those bundles to avoid overloading the web server from which they are being downloaded.
    
    When running parallelized, terse output will be produced.
    
    Terse output key:
    
        B - a new bundle is being examined
        ✔ - a data file has been checked and is already in place
        ! - a data file could not be found
        C - a data file was copied from another S3 bucket to the target location
        ⬇ - a data file was downloaded from S3 (so checksum could be recomputed)
        ↓ - a data file was downloaded from the internet
        ⬆ - a data file was upload to the target bucket
        + - missing checksums where added to an already uploaded file 
        ✓ - a metadata file has been checked and is already in place
        ↑ - a metadata file was uploaded to the target location
        
        e.g. this bundle is already done: B✔✓✓✓✓
             this bundle was new:         B↓⬆↑↑↑↑
    
    When running parallelized you can still generate verbose output with the --log option. 
"""

# Executor complains if it is an object attribute, so we make it global.
executor = None


class Main:

    DEFAULT_BUCKET = 'org-humancellatlas-data-bundle-examples'

    def __init__(self):
        self._parse_args()
        self._setup_ssl_context(self.args.skip_ssl_cert_verification)
        if self.args.bundle:
            self.stage_bundle(LocalBundle(self.args.bundle))
        else:
            logger.output(f"\nStaging bundles under \"{self.args.bundles}\":\n")
            bundles = list(LocalBundle.bundles_under(self.args.bundles))
            bundles.sort()
            self.stage_bundles(bundles)
        print("")

    def stage_bundles(self, bundles):
        self.total_bundles = len(bundles)
        if self.args.jobs > 1:
            self.stage_bundles_in_parallel(bundles)
        else:
            self.stage_bundles_serially(bundles)

    def stage_bundles_serially(self, bundles):
        """ This produces much better error messages that operating under ProcessPoolExecutor """
        bundle_number = 0
        for bundle in bundles:
            bundle_number += 1
            self.stage_bundle(bundle, bundle_number)

    def stage_bundles_in_parallel(self, bundles):
        global executor
        signal.signal(signal.SIGINT, self.signal_handler)
        executor = ProcessPoolExecutor(max_workers=self.args.jobs)
        bundle_number = 0
        for bundle in bundles:
            bundle_number += 1
            executor.submit(self.stage_bundle, bundle, bundle_number)
        executor.shutdown()

    def stage_bundle(self, bundle, bundle_number=None):
        comment = f"({bundle_number}/{self.total_bundles})" if bundle_number else ""
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        BundleStager(bundle, self.args.target_bucket).stage(comment)

    def _parse_args(self):
        parser = argparse.ArgumentParser(description="Stage example bundles in S3.",
                                         usage='%(prog)s [options]',
                                         epilog="Default action is to stage all bundles under ./import/")
        parser.add_argument('--target-bucket', metavar="<s3-bucket-name>", default=self.DEFAULT_BUCKET,
                            help="stage files in this bucket")
        parser.add_argument('--bundle', default=None, metavar="path/to/bundle",
                            help="stage single bundle at this path")
        parser.add_argument('--bundles', default='import', metavar="path",
                            help="stage bundles under this path (must not include 'bundles')")
        parser.add_argument('-q', '--quiet', action='store_true', default=False,
                            help="silence is golden")
        parser.add_argument('-t', '--terse', action='store_true', default=False,
                            help="terse output, one character per file")
        parser.add_argument('-l', '--log', default=None,
                            help="log verbose output to this file")
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help="parallelize with this many jobs")
        parser.add_argument('--skip-ssl-cert-verification', default=False, action='store_true',
                            help="don't attempt to verify SSL certificates")
        self.args = parser.parse_args()
        if self.args.jobs > 1:
            quiet = True
            terse = True
        else:
            quiet = self.args.quiet
            terse = self.args.terse

        logger.configure(self.args.log, quiet=quiet, terse=terse)

    @staticmethod
    def signal_handler(signal, frame):
        global executor
        print('Shutting down...')
        executor.shutdown()
        sys.exit(0)

    @staticmethod
    def _setup_ssl_context(skip_ssl_cert_verification):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE if skip_ssl_cert_verification else ssl.CERT_REQUIRED


# run the class
if __name__ == '__main__':
    runner = Main()
