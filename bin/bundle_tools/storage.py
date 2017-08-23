import os, uuid
from datetime import datetime
import boto3
import requests
from urllib3.util import Url
# from hca import api as hca_api
from .parallel_logger import logger
from .utils import sizeof_fmt


class BundleStorer:

    def __init__(self, bundle, dss_url):
        self.bundle = bundle
        self.file_info = []
        self.api = DataStoreAPI(dss_url)

    def store_bundle(self):
        try:
            logger.output(f"\n{self.bundle.path}:", progress_char="B")
            self._assign_uuids()
            self.bundle.submission_info.save()
            self._store_files()
            self._register_bundle()
            logger.flush()
        except DSSAPIError as e:
            logger.output(f"\n\nERROR attempting to store bundle {self.bundle.path}: {str(e)}\n",
                          progress_char="!", flush=True)

    def _store_files(self):
        for file in self.bundle.files.values():
            size_message = f" ({sizeof_fmt(file.size)})" if not file.is_metadata() else ""
            logger.output(f"\n  storing file {file.name}{size_message} as {file.uuid}...")
            version = self.api.put_file(self.bundle.uuid, file.uuid, file.staged_url, method='rest')
            self.file_info.append({
                'name': file.name,
                'uuid': file.uuid,
                'version': version,
                'indexed': file.is_metadata()
            })
            logger.output(f" {version}", progress_char="s")

    def _assign_uuids(self):
        if not self.bundle.uuid:
            self.bundle.uuid = str(uuid.uuid4())
        for file in self.bundle.files.values():
            if not file.uuid:
                file.uuid = str(uuid.uuid4())

    def _register_bundle(self):
        logger.output(f"\n  registering bundle {self.bundle.uuid}... ")
        version = self.api.put_bundle(self.bundle.uuid, self.file_info)
        logger.output(version, progress_char='✔︎')


class StagedBundleFinder:

    def __init__(self):
        self.bundle_paths = list()
        self.s3 = boto3.client('s3')

    def paths_of_bundles_under(self, s3url: Url) -> list:
        # Assumption: bundles are stored at **/bundles/bundleX/
        logger.output(f"\nFinding bundles under {str(s3url)}...")
        self._search_for_bundles_in_folder(bucket=s3url.host, root_path=s3url.path.lstrip('/'))
        logger.output("\n")
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
            logger.progress(".", flush=True)
            if 'CommonPrefixes' in page:
                for obj in page['CommonPrefixes']:
                    yield obj['Prefix']

    @staticmethod
    def _is_bundle_home(path: str) -> bool:
        return os.path.basename(path.rstrip('/')) == 'bundles'


class DSSAPIError(RuntimeError):
    pass


class DataStoreAPI:

    DEFAULT_DSS_URL = "https://hca-dss.czi.technology/v1"
    FAKE_CREATOR_UID = 104

    def __init__(self, endpoint_url=DEFAULT_DSS_URL):
        self.dss_url = endpoint_url

    def put_file(self, bundle_uuid: str, file_uuid: str, file_location: str, method='rest'):
        if method == 'rest':
            return self._put_file_via_rest(bundle_uuid, file_uuid, file_location)
        else:
            return self._put_file_via_python_bindings(bundle_uuid, file_uuid, file_location)

    def put_bundle(self, bundle_uuid, file_info):
        payload = {
            'creator_uid': self.FAKE_CREATOR_UID,
            'files': file_info
        }
        url = f"{self.dss_url}/bundles/{bundle_uuid}"
        params = {'version': datetime.now().isoformat(), 'replica': 'aws'}
        response = requests.put(url, params=params, json=payload)
        if response.status_code != 201:
            raise DSSAPIError(f"put({url}, {params}, {payload}) returned status {response.status_code}: {response.text}")
        return response.json()['version']

    def _put_file_via_rest(self, bundle_uuid: str, file_uuid: str, file_location: str):
        # The HCA Python API does not currently respect api_url so we use Requests.
        # It is also very noisy and turns on boto3 debugging output, which results in a poor UX.
        payload = {
            'bundle_uuid': bundle_uuid,
            'creator_uid': 104,
            'source_url': file_location,
        }
        url = f"{self.dss_url}/files/{file_uuid}"
        params = {'version': datetime.now().isoformat()}
        response = requests.put(url, params=params, json=payload)
        if response.status_code != 201:
            raise DSSAPIError(f"put({url}, {params}, {payload}) returned status {response.status_code}: {response.text}")
        return response.json()['version']

    def _put_file_via_python_bindings(self, bundle_uuid: str, file_uuid: str, file_location: str):
        response = hca_api.put_files(uuid=file_uuid,
                                     source_url=file_location,
                                     creator_uid=104,
                                     bundle_uuid=bundle_uuid,
                                     api_url=self.dss_url
                                     )
        if response.status_code != 201:
            print(f"ERROR: put_files() returned {response.status_code}: {response.text}")
            exit(1)
        return response

