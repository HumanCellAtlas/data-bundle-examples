import os, sys, uuid
from datetime import datetime
import boto3
import requests
from urllib3.util import Url
from hca import api as hca_api


class BundleStorer:

    def __init__(self, bundle, dss_url):
        self.bundle = bundle
        self.api = DataStoreAPI(dss_url)

    def store_bundle(self):
        self._assign_uuids()
        self.bundle.submission_info.save()
        self._store_files()

    def _store_files(self):
        for file in self.bundle.files.values():
            print(f"Storing file {file.name}...")
            self.api.store_file_via_rest(self.bundle.uuid, file.uuid, file.staged_url)

    def _assign_uuids(self):
        if not self.bundle.uuid:
            self.bundle.uuid = str(uuid.uuid4())
        for file in self.bundle.files.values():
            if not file.uuid:
                file.uuid = str(uuid.uuid4())


class StagedBundleFinder:
    """
    Assumption: bundles are stored at **/bundles/bundleX/
    """

    def __init__(self):
        self.bundle_paths = list()
        self.s3 = boto3.client('s3')

    def paths_of_bundles_under(self, s3url: Url) -> list:
        self.progress("Finding bundles: ")
        self._search_for_bundles_in_folder(bucket=s3url.netloc, root_path=s3url.path[1:])
        self.progress("\n")
        return self.bundle_paths

    def _search_for_bundles_in_folder(self, bucket: str, root_path: str) -> list:
        for folder_path in self._subfolders_in_folder(bucket, root_path):
            if self._is_bundle_home(root_path):
                self.bundle_paths.append(folder_path.rstrip('/'))
            else:
                self._search_for_bundles_in_folder(bucket, folder_path)

    def _subfolders_in_folder(self, bucket: str, folder_path: str):
        paginator = self.s3.get_paginator('list_objects')
        for page in paginator.paginate(Bucket=bucket, Prefix=folder_path, Delimiter='/'):
            self.progress(".")
            if 'CommonPrefixes' in page:
                for obj in page['CommonPrefixes']:
                    yield obj['Prefix']

    @staticmethod
    def _is_bundle_home(path: str) -> bool:
        return os.path.basename(path.rstrip('/')) == 'bundles'

    @staticmethod
    def progress(string):
        sys.stdout.write(string)
        sys.stdout.flush()


class DataStoreAPI:

    DEFAULT_DSS_URL = "https://hca-dss.czi.technology/v1"

    def __init__(self, endpoint_url=DEFAULT_DSS_URL):
        self.dss_url = endpoint_url

    def store_file_via_rest(self, bundle_uuid: str, file_uuid: str, file_location: str):
        # The HCA Python API does not currently respect api_url so we use Requests
        payload = {
            'bundle_uuid': bundle_uuid,
            'creator_uid': 104,
            'source_url': file_location,
        }
        url = f"{self.dss_url}/files/{file_uuid}"
        params = {'version': datetime.now().isoformat()}
        response = requests.put(url, params=params, json=payload)
        if response.status_code != 201:
            print(f"ERROR: put_files() returned {response.status_code}: {response.text}")
            exit(1)

    def store_file_via_python_bindings(self, bundle_uuid: str, file_uuid: str, file_location: str):
        response = hca_api.put_files(uuid=file_uuid,
                                     source_url=file_location,
                                     creator_uid=104,
                                     bundle_uuid=bundle_uuid,
                                     api_url=self.dss_url
                                     )
        if response.status_code != 201:
            print(f"ERROR: put_files() returned {response.status_code}: {response.text}")
            exit(1)


