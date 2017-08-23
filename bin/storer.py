#!/usr/bin/env python3.6

import argparse
from urllib3.util import parse_url
from bundle_tools import StagedBundle, StagedBundleFinder, BundleStorer, DataStoreAPI

"""
    Store staged bundles in the HCA DSS Data Store
         
    TODO:
     * Switch to submission.json
     * persist UUIDs
     * Parallelize
"""


class Main:

    def __init__(self):
        parser = argparse.ArgumentParser(description="Store staged bundles in HCA Data Store")
        parser.add_argument('--bundles',
                            help="S3 URL of staged bundles")
        parser.add_argument('--bundle',
                            help="S3 URL of a single staged bundle")
        parser.add_argument('--dss-endpoint', default=DataStoreAPI.DEFAULT_DSS_URL,
                            help="URL of DSS API endpoint")
        self.args = parser.parse_args()
        if self.args.bundle:
            self._store_bundle(self.args.bundle)
        if self.args.bundles:
            self._store_bundles(self.args.bundles)

    def _store_bundles(self, bundles_url):
        url = parse_url(bundles_url)
        bundle_paths = StagedBundleFinder().paths_of_bundles_under(url)
        for path in bundle_paths:
            bundle = StagedBundle(bucket=url.netloc, path=path.lstrip('/'))
            BundleStorer(bundle, self.args.dss_endpoint).store_bundle()

    def _store_bundle(self, bundle_url):
        url = parse_url(bundle_url)
        bundle = StagedBundle(bucket=url.netloc, path=url.path.lstrip('/'))
        BundleStorer(bundle, self.args.dss_endpoint).store_bundle()


if __name__ == '__main__':
    runner = Main()
