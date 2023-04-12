import functools
import hashlib
import json
import mimetypes
import os
import pathlib
import requests
import ssl
import urllib3
import sys

from time import sleep
from datetime import datetime, timedelta
from binaryornot.check import is_binary
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
def log_output_duplicate(mes):
    print(mes)
    print(mes, file=sys.__stdout__, flush=True)

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
            self.access_token = api_key
            self.session.headers.update(self.access_header)
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

    def download(self, file, path=None, folder=None):
        if folder is not None:
            path = pathlib.Path(folder) / file['name']
        path.parent.mkdir(parents=True, exist_ok=True)

        log_output_duplicate(f"[{timestamp()}] Downloading {path}...")

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
                log_output_duplicate(f'[{timestamp()}] {path}: wrong content checksum. retrying...')
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

#---------------------------------------------------------------------------
class ServerProxy (Server):
    """
    Wrapper for Server class
    """
    def __init__(self, api_key:str, project:int, input_node:int, output_node:int, api_server:str=None):
        """
        Args:
            api_key (str): API key
            project (int): Project-server ID
            input_node (int): Input node ID of project-server
            output_node (int): Output node ID of project-server
            api_server (str, optional): API server URL. Defaults to None.
        """
        self.project = project
        self.input_node = input_node
        self.output_node = output_node

        super().__init__(api_key=api_key, api_server=api_server)

    def getServer(prefix:str, api_server:str=None):
        """
        Get ServerProxy object.

        Args:
            prefix (str): API key secrets common prefix name
            api_server (str,optional): API server URL. Defaults to None.

        Returns:
             ServerProxy object
        """

        try:
            from rndflow.job import secret
        except:
            pass

        api_key= secret(f'{prefix}_token')
        project =  secret(f'{prefix}_project')
        input_node = secret(f'{prefix}_input')
        output_node = secret(f'{prefix}_output')

        return ServerProxy(api_key, project, input_node, output_node, api_server)

    def getLastDataLayer(self)->int:
        """
        Get the ID of the last data layer available to the user.
        Returns:
            int: data layer ID
        """
        layer = self.get(f'/projects/{self.project}/data_layers/last')
        return layer['id']

    def getDataLayers(self)->list:
        """
        Get available data layers.
        Returns:
            list of dict: list of data layers ID
        """
        rez = self.get(f'/projects/{self.project}/data_layers')
        layers = list(map(lambda x: x['id'], rez))
        return layers


    def postPackage(self, layer: int, label: str, fields: dict)->int:
        """
        Send package to input node of the project-server.

        Args:
            layer (int): data layerd ID
            label (str): package label
            fields (dict): package fields

        Returns:
            int: package ID
        """
        package=dict(label=label,felds=fields)
        return self.postPackage(layer, package)

    def postPackage(self, layer: int, package: dict)->int:
        """
        Send package to input node of the project-server.

        Args:
            layer (int): data layerd ID
            package (dict): package

        Returns:
            int: package ID
        """
        p = self.post(f'/projects/{self.project}/nodes/{self.input_node}/packages',
                params=dict(data_layer_id=layer,),
                json=package)
        return p['id']

    def searchByMaster(self, layer: int, master: int, page: int=1, page_size: int=1):
        """
        Seach package in the output node of the project-server by the master package id.

        Args:
            layer (int): data layerd ID
            master (int): master package id
            page (int): page number, defaults to 1.
            page_size (int): packages count on page, defaults to 1.

        Returns:
            dict: result dictionary
        """
        return self.post(f'/projects/{self.project}/nodes/{self.output_node}/packages/search',
        params=dict(
            data_layer_id=layer,
            page=page,
            page_size=page_size
            ),
        json=dict(
            master_id=master
            ))

    def waitResult(self, layer: int, master: int, timeout=timedelta(minutes=5), retry_pause:int=5, page: int=1, page_size: int=10)->list:
        """

        Wait for the results packages in the output node of the project-server.

        Args:
            layer (int): data layerd ID
            master (int):  master package id
            timeout (timedelta, optional): Timeout. Defaults to timedelta(minutes=5).
            retry_pause (int, optional): Pause between requests to output node. Defaults to 5.
            page (int): page number, defaults to 1.
            page_size (int): packages count on page, defaults to 10.

        Raises:
            Exception: Timeout exception

        Returns:
            list: packages list, total packages count
        """
        borderTime = datetime.now() + timeout

        ready = False
        while not ready and datetime.now() < borderTime:
            results = self.searchByMaster(layer, master, page, page_size)
            for result in results['items']:
                ready = True
                break
            else:
                sleep(retry_pause)

        if not ready:
            raise Exception('Timeout!')

        return results['items'], results['total']

    def waitOneResult(self, layer: int, master: int, timeout=timedelta(minutes=5), retry_pause:int=5)->list:
        """

        Wait for the one result package in the output node of the project-server.

        Args:
            layer (int): data layerd ID
            master (int):  master package id
            timeout (timedelta, optional): Timeout. Defaults to timedelta(minutes=5).
            retry_pause (int, optional): Pause between requests to output node. Defaults to 5.

        Raises:
            Exception: Timeout exception

        Returns:
            list: package ID, package fields
        """
        items, _ = self.waitResult(layer, master, timeout, retry_pause, 1, 1)
        return items[0]['id'], items[0]['fields']

    def getFilesList(self, ident: int)->list:
        """
        Get files list of package
        Args:
            ident (int): package ID

        Returns:
            list: files list
        """
        return self.get(f'/projects/{self.project}/nodes/{self.output_node}/packages/{ident}/files')

    def waitOneResultAndFiles(self, layer, master, timeout=timedelta(minutes=5), retry_pause:int=5)->list:
        """
        Wait for the one result package in the output node of the project-server.

        Args:
            layer (int): data layerd ID
            master (int):  master package id
            timeout (timedelta, optional): Timeout. Defaults to timedelta(minutes=5).
            retry_pause (int, optional): Pause between requests to output node. Defaults to 5.

        Raises:
            Exception: Timeout exception

        Returns:
            list: package ID, package fields list, package files list
        """
        ident, fields = self.waitOneResult(layer, master, timeout, retry_pause)
        files = self.getFilesList(ident)
        return ident, fields, files
