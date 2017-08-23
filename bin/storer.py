#!/usr/bin/env python3.6

import argparse, signal, sys
from concurrent.futures import ProcessPoolExecutor
from urllib3.util import parse_url, Url
from bundle_tools import StagedBundle, StagedBundleFinder, BundleStorer, DataStoreAPI
from utils import logger

"""
    Store staged bundles in the HCA DSS Data Store
"""


# Executor complains if it is an object attribute, so we make it global.
executor = None


class Main:

    DEFAULT_STAGED_BUNDLES_URL = "s3://org-humancellatlas-data-bundle-examples/import"

    def __init__(self):
        parser = argparse.ArgumentParser(description="Store staged bundles in HCA Data Store")
        parser.add_argument('--bundles', default=self.DEFAULT_STAGED_BUNDLES_URL,
                            help="S3 URL of staged bundles")
        parser.add_argument('--bundle',
                            help="S3 URL of a single staged bundle")
        parser.add_argument('--smaller', type=int, default=1024*1024,  # 1 TB
                            help="only store bundles with data files smaller than this many MB")
        parser.add_argument('--dss-endpoint', default=DataStoreAPI.DEFAULT_DSS_URL,
                            help="URL of DSS API endpoint")
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help="parallelize with this many jobs (implies --terse)")
        parser.add_argument('-q', '--quiet', action='store_true', default=False,
                            help="silence is golden")
        parser.add_argument('-t', '--terse', action='store_true', default=False,
                            help="terse output, one character per file")
        parser.add_argument('-l', '--log', default=None,
                            help="log verbose output to this file")
        self.args = parser.parse_args()
        if self.args.jobs > 1:
            quiet = True
            terse = True
        else:
            quiet = self.args.quiet
            terse = self.args.terse
        logger.configure(self.args.log, quiet=quiet, terse=terse)
        if self.args.bundle:
            self._store_bundle(parse_url(self.args.bundle))
        else:
            self._store_bundles(parse_url(self.args.bundles))

    def _store_bundles(self, bundles_url: Url):
        bundle_paths = StagedBundleFinder().paths_of_bundles_under(bundles_url)
        if self.args.jobs > 1:
            self.store_bundles_in_parallel(bundles_url, bundle_paths)
        else:
            for path in bundle_paths:
                self._store_bundle(Url(scheme=bundles_url.scheme, host=bundles_url.host, path=path.lstrip('/')))

    def store_bundles_in_parallel(self, bundles_url: Url, bundle_paths: list):
        global executor
        signal.signal(signal.SIGINT, self.signal_handler)
        executor = ProcessPoolExecutor(max_workers=self.args.jobs)
        bundle_number = 0
        for path in bundle_paths:
            bundle_number += 1
            executor.submit(self._store_bundle,
                            Url(scheme=bundles_url.scheme, host=bundles_url.host, path=path.lstrip('/')))
        executor.shutdown()

    def _store_bundle(self, bundle_url):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        bundle = StagedBundle(bundle_url)
        if bundle.all_files_are_smaller_than(self.args.smaller):
            BundleStorer(bundle, self.args.dss_endpoint).store_bundle()
        else:
            logger.output(f"\nSkipping {bundle.path} - exceeds sized requirement", progress_char='-', flush=True)

    @staticmethod
    def signal_handler(signal, frame):
        global executor
        print('Shutting down...')
        executor.shutdown()
        sys.exit(0)


if __name__ == '__main__':
    runner = Main()
