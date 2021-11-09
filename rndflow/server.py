import functools
import hashlib
import json
import mimetypes
import os
import pathlib
import requests
import ssl
import urllib3

from binaryornot.check import is_binary
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context

from .config import Settings

#---------------------------------------------------------------------------
def timestamp():
    return datetime.utcnow().replace(microsecond=0).isoformat(sep=' ')

#---------------------------------------------------------------------------
def response_json(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        r = fn(*args, **kwargs)
        if r.status_code != requests.codes.ok:
            print(*args[1:], r.text)
        r.raise_for_status()
        return r.json()
    return wrapper

#---------------------------------------------------------------------------
def file_hash(path):
    chunk = 65536

    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(chunk), b''):
            h.update(b)

    return h.hexdigest()

#---------------------------------------------------------------------------
class Server:
    def __init__(self, api_server=None, api_key=None):
        cfg = Settings()

        if api_server is None:
            api_server = cfg.rndflow_api_server
            assert api_server, 'API server URL is not set'

        self.base_url = f'{api_server}/api'

        self.session = requests.Session()
        self.raw_session = requests.Session()

        # https://www.peterbe.com/plog/best-practice-with-retries-with-requests
        adapter = HTTPAdapter(max_retries=Retry(
            total=3, read=3, connect=3,
            backoff_factor=0.3, status_forcelist=(502,504)))

        for session in (self.session, self.raw_session):
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            session.verify = False

        self.access_token = None
        self.refresh_token = None

        if api_key is not None:
            self.refresh_token = api_key
            self.refresh_url = f'{self.base_url}/auth/refresh'
        else:
            self.refresh_token = cfg.rndflow_refresh_token
            self.refresh_url = f'{self.base_url}/executor_api/auth/refresh'

        self.refresh_tokens()
        self.session.hooks['response'].append(self.refresh_as_needed)

    @property
    def access_header(self):
        return dict(Authorization=f'Bearer {self.access_token}')

    @property
    def refresh_header(self):
        return dict(Authorization=f'Bearer {self.refresh_token}')

    def refresh_tokens(self):
        r = self.raw_session.post(self.refresh_url,
                headers=self.refresh_header)
        if r.status_code != requests.codes.ok:
            print(self.refresh_url, r.text)
        r.raise_for_status()
        data = r.json()

        self.access_token = data['access_token']
        self.refresh_token = data['refresh_token']

        self.session.headers.update(self.access_header)

    def refresh_as_needed(self, response, *args, **kwargs):
        if response.status_code == requests.codes.unauthorized and self.refresh_token:
            self.refresh_tokens()

            request = response.request
            request.headers.update(self.access_header)

            return self.session.send(request)

    @response_json
    def get(self, resource, *args, **kwargs):
        return self.session.get(f'{self.base_url}{resource}', *args, **kwargs)
        
    @response_json
    def post(self, resource, *args, **kwargs):
        return self.session.post(f'{self.base_url}{resource}', *args, **kwargs)
        
    @response_json
    def put(self, resource, *args, **kwargs):
        return self.session.put(f'{self.base_url}{resource}', *args, **kwargs)

    @response_json
    def delete(self, resource, *args, **kwargs):
        return self.session.delete(f'{self.base_url}{resource}', *args, **kwargs)

    def download(self, path, file):
        path = pathlib.Path(path) / file['name']
        path.parent.mkdir(parents=True, exist_ok=True)

        print(f'[{timestamp()}] Downloading {path}...')

        ntries = 2

        while True:
            r = self.raw_session.get(file['content'], stream=True)
            r.raise_for_status()

            h = hashlib.sha256()
            with open(path, 'wb') as f:
                for chunk in r:
                    h.update(chunk)
                    f.write(chunk)

            ntries -= 1

            if h.hexdigest() == file['content_hash']:
                break
            elif ntries > 0:
                print(f'[{timestamp()}] {path}: wrong content checksum. retrying...')
            else:
                raise Exception(f'{path}: wrong content checksum.')

        if file['is_executable']:
            os.chmod(path, 0o770)

    def upload_project_file(self, project, path, name=None):
        path     = pathlib.Path(path)
        name     = name or path.name
        size     = path.stat().st_size
        f_hash   = file_hash(path)
        binary   = is_binary(str(path))
        f_type,_ = mimetypes.guess_type(str(path))
        if f_type is None:
            f_type = 'application/x-binary' if binary else 'text/plain'

        link = self.put(f'/projects/{project}/objects/{f_hash}')
        if link is not None:
            with open(path, 'rb') as f:
                self.raw_session.put(link, data=f, headers={
                    'Content-Type': f_type,
                    'Content-Length': str(size)
                    }).raise_for_status()

        return dict(
            name          = name,
            content_hash  = f_hash,
            type          = f_type,
            is_executable = os.access(path, os.X_OK),
            is_binary     = binary,
            size          = size
            )
