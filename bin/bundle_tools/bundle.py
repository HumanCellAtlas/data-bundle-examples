import glob, json, os
from urllib3.util import Url
from .utils import file_size, MB
import bundle_tools.submission


class File:
    def __init__(self, name=None, bundle=None, uuid=None, size=None, content_type=None, origin_url=None, staged_url=None):
        self.name = name
        self.bundle = bundle
        self.uuid = uuid
        self.size = size
        self.content_type = content_type
        self.origin_url = origin_url
        self.staged_url = staged_url
        self.checksums = {}

    def __eq__(self, other) -> bool:
        return self.bundle == other.bundle and self.name == other.name

    def is_metadata(self) -> bool:
        return self.content_type == 'application/json'

    def path(self):
        return f"{self.bundle.path}/{self.name}"


class MetadataFile(File):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content_type = self.content_type or 'application/json'


class DataFile(File):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content_type = self.content_type or 'application/octet-stream'


class Bundle:

    def __init__(self, path=None):
        self.path = path
        self.files = dict()  # 'file name': File
        self.submission_info = None

    def __lt__(self, other):
        return self.path < other.path

    def add_file(self, file: File):
        file.bundle = self
        self.files[file.name] = file


class LocalBundle(Bundle):

    MANIFEST_FILENAME = 'manifest.json'
    BUNDLE_HOME_DIRNAME = 'bundles'  # Folders containing bundles

    @classmethod
    def bundles_under(cls, folder):
        for bundle_home in glob.glob(f"{folder}/**/{cls.BUNDLE_HOME_DIRNAME}", recursive=True):
            bundle_dirs = os.listdir(bundle_home)
            for bundle_dir in bundle_dirs:
                bundle_path = os.path.join(bundle_home, bundle_dir)
                yield cls(bundle_path)

    def __init__(self, local_path):
        super().__init__(path=local_path)
        self.manifest = None

    def enumerate_local_metadata_files(self):
        for path in glob.glob(f"{self.path}/*.json"):
            name = os.path.basename(path)
            if name == self.MANIFEST_FILENAME:
                self.manifest = path
            else:
                if name not in self.files:
                    self.add_file(MetadataFile(name=name, size=os.stat(path).st_size))

    def enumerate_data_files_using_manifest(self):
        if self.manifest is None:
            raise RuntimeError(f"Bundle {self.path} has no {self.MANIFEST_FILENAME}")
        with open(self.manifest, 'r') as data:
            manifest = json.load(data)
            for fileinfo in manifest['files']:
                if fileinfo['name'] not in self.files:
                    self.add_file(
                        DataFile(name=fileinfo['name'],
                                 size=(file_size(f"{manifest['dir']}/{fileinfo['name']}")),
                                 origin_url=f"{manifest['dir']}/{fileinfo['name']}")
                    )


class StagedBundle(Bundle):

    def __init__(self, location: Url):
        self.bucket = location.netloc
        self.uuid = None
        super().__init__(location.path.rstrip('/'))
        self.submission_info = bundle_tools.submission.SubmissionInfo(self.bucket, self)
        self.submission_info.load()

    def all_files_are_smaller_than(self, this_many_mb):
        f = filter(lambda file: file.size > (this_many_mb * MB), self.files.values())
        files_that_are_too_large = list(f)
        return len(files_that_are_too_large) == 0
