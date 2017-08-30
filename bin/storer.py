#!/usr/bin/env python3.6

"""
Store staged bundles in the HCA DCP Data Store.

Bundles must have been previously staged.  You must prvide the S3 URL of a
bundle or bundle hierarchy (hierarchies must have at least one folder named
'bundles' within them) , e.g.

bin/storer.py --bundles s3://org-humancellatlas-data-bundle-examples/import/10x

bin/storer.py --bundle s3://org-humancellatlas-data-bundle-examples/import/geo/GSE75478/bundles/bundle145
"""

import argparse, signal, sys
from concurrent.futures import ProcessPoolExecutor
from urllib3.util import parse_url, Url
from bundle_tools import logger, StagedBundle, StagedBundleFinder, BundleStorer, DataStoreAPI

# Executor complains if it is an object attribute, so we make it global.
executor = None


class Main:

    DEFAULT_STAGED_BUNDLES_URL = "s3://org-humancellatlas-data-bundle-examples/import"

    def __init__(self):
        parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('--bundles', default=self.DEFAULT_STAGED_BUNDLES_URL,
                            help="S3 URL of staged bundles")
        parser.add_argument('--bundle',
                            help="S3 URL of a single staged bundle")
        parser.add_argument('--smaller', type=int, default=1024*1024,  # 1 TB
                            help="only store bundles with data files smaller than this many MB")
        parser.add_argument('--dss-endpoint', default=DataStoreAPI.DEFAULT_DSS_URL,
                            help="URL of DSS API endpoint")
        parser.add_argument('-r', '--use-rest-api', action='store_true', default=False,
                            help="use REST API directly, instead of python bindings")
        parser.add_argument('-j', '--jobs', type=int, default=1,
                            help="parallelize with this many jobs (implies --terse)")
        parser.add_argument('-q', '--quiet', action='store_true', default=False,
                            help="don't produce verbose output on STDOUT")
        parser.add_argument('-t', '--terse', action='store_true', default=False,
                            help="terse output, one character per file")
        parser.add_argument('-i', '--report-task-ids', action='store_true', default=False,
                            help="when a 202 (ACCEPTED) response is received, print task ID")
        parser.add_argument('-l', '--log', default=None,
                            help="log verbose output to this file")
        self.args = parser.parse_args()
        if self.args.jobs > 1:
            self.args.quiet = True
            self.args.terse = True
        logger.configure(self.args.log, quiet=self.args.quiet, terse=self.args.terse)
        self.storer_options = {'use_rest_api': self.args.use_rest_api, 'report_task_ids': self.args.report_task_ids}

        if self.args.bundle:
            self._store_bundle(parse_url(self.args.bundle))
            print("\n")
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
            BundleStorer(bundle, self.args.dss_endpoint, **self.storer_options).store_bundle()
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
