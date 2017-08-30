import os, uuid, time
from datetime import datetime

import boto3
import requests
from urllib3.util import Url

from .parallel_logger import logger
from .utils import sizeof_fmt, measure_duration_and_rate


class BundleStorer:

    def __init__(self, bundle, dss_url, use_rest_api=False, report_task_ids=False):
        self.bundle = bundle
        self.file_info = []
        driver = 'rest' if use_rest_api else 'python'
        self.api = DataStoreAPI(driver=driver, endpoint_url=dss_url, report_task_ids=report_task_ids)

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
            version, duration, rate = measure_duration_and_rate(self.api.put_file,
                                                                self.bundle.uuid, file.uuid, file.staged_url,
                                                                size=file.size)
            self.file_info.append({
                'name': file.name,
                'uuid': file.uuid,
                'version': version,
                'indexed': file.is_metadata()
            })
            logger.output(" %s (%.1f sec, %.1f MiB/sec)" % (version, duration, rate), progress_char="s")

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
            if 'CommonPrefixes' in page:
                for obj in page['CommonPrefixes']:
                    yield obj['Prefix']

    @staticmethod
    def _is_bundle_home(path: str) -> bool:
        return os.path.basename(path.rstrip('/')) == 'bundles'


class DSSAPIError(RuntimeError):
    pass


class DSSDriver:

    FAKE_CREATOR_UID = 104
    DEFAULT_DSS_REPLICA = 'aws'
    BACKOFF_FACTOR = 1.618

    def __init__(self, endpoint_url, report_task_ids=False):
        self.dss_url = endpoint_url
        self.report_task_ids = report_task_ids

    def put_file(self, bundle_uuid: str, file_uuid: str, file_location: str):
        raise NotImplementedError()

    def put_bundle(self, bundle_uuid: str, file_info: list):
        raise NotImplementedError()


class DSSpythonDriver(DSSDriver):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def put_file(self, bundle_uuid: str, file_uuid: str, file_location: str):
        from hca import api as hca_api
        response = hca_api.put_files(uuid=file_uuid,
                                     source_url=file_location,
                                     creator_uid=104,
                                     bundle_uuid=bundle_uuid,
                                     api_url=self.dss_url
                                     )
        if response.status_code != 201:
            print(f"ERROR: put_files() returned {response.status_code}: {response.text}")
            exit(1)
        return response.json()['version']

    def put_bundle(self, bundle_uuid: str, file_info: list):
        from hca import api as hca_api
        response = hca_api.put_bundles(bundle_uuid, self.DEFAULT_DSS_REPLICA, self.FAKE_CREATOR_UID, file_info)
        if response.status_code != 201:
            print(f"ERROR: put_files() returned {response.status_code}: {response.text}")
            exit(1)
        return response.json()['version']


class DSSrestDriver(DSSDriver):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def put_file(self, bundle_uuid: str, file_uuid: str, file_location: str):
        payload = {
            'bundle_uuid': bundle_uuid,
            'creator_uid': 104,
            'source_url': file_location,
        }
        url = f"{self.dss_url}/files/{file_uuid}"
        params = {'version': datetime.now().isoformat()}
        response = requests.put(url, params=params, json=payload)
        if response.status_code == 201:
            return response.json()['version']
        elif response.status_code == 202:
            if self.report_task_ids:
                logger.output(f"\n    ACCEPTED: task_id={response.json()['task_id']}, waiting.")
            response = self._wait_for_file_to_exist(file_uuid)
            return response.headers['X-DSS-VERSION']
        else:
            raise DSSAPIError(f"put({url}, {params}, {payload}) returned status {response.status_code}: {response.text}")

    def head_file(self, file_uuid: str, version: str=None):
        if version:
            url = f"{self.dss_url}/files/{file_uuid}?version={version}"
        else:
            url = f"{self.dss_url}/files/{file_uuid}"
        params = {'replica': self.DEFAULT_DSS_REPLICA}
        response = requests.head(url, params=params)
        return response

    def put_bundle(self, bundle_uuid: str, file_info: list):
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

    def _wait_for_file_to_exist(self, file_uuid, timeout_seconds=30*60):
        timeout = time.time() + timeout_seconds
        wait = 1.0
        while time.time() < timeout:
            response = self.head_file(file_uuid)
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                time.sleep(wait)
                logger.output(".", flush=True)
                wait = min(60.0, wait * self.BACKOFF_FACTOR)
            else:
                raise RuntimeError(response)
        else:
          raise RuntimeError(f"File {file_uuid} did not appear within {timeout_seconds} seconds")


class DataStoreAPI:

    DEFAULT_DSS_URL = "https://hca-dss.czi.technology/v1"

    def __init__(self, driver='rest', endpoint_url=DEFAULT_DSS_URL, report_task_ids: bool=False):
        driver_name = f"DSS{driver}Driver"
        self.driver = eval(driver_name)(endpoint_url=endpoint_url, report_task_ids=report_task_ids)

    def put_file(self, *args, **kwargs):
        return self.driver.put_file(*args, **kwargs)

    def put_bundle(self, *args, **kwargs):
        return self.driver.put_bundle(*args, **kwargs)
