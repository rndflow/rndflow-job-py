import json
import mimetypes
import os
import subprocess
import time
import traceback

from collections import deque
from pathlib import Path
from textwrap import dedent
from threading import Event, Thread, Timer

import argparse
from binaryornot.check import is_binary

from .server import Server, file_hash
from .config import Settings

from .logger import make_file_stdout_logger

#---------------------------------------------------------------------------------------
def timer_or_event(duration , event):
    timer = Timer(duration, lambda: event.set())  # pylint: disable=unnecessary-lambda
    timer.start()
    event.wait()
    timer.cancel()  # Stop the timer if job finished
    event.clear()

#---------------------------------------------------------------------------------------
class Job:

    def __init__(self, host: str, job_id: int):
        self.job = None
        self.status = None
        self.job_id = job_id
        self.server = Server(host)
        self.cfg = Settings()

        self.root = Path(str(job_id)).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

        self.log_file = self.root / f'{self.job_id}.log'
        self.logger = make_file_stdout_logger(self.log_file)

        self.data_upload = False

        self.done = Event()
        self.heartbeat_sleep = Event()
        self.heartbeat_thread = Thread(target=self.heartbeat)
        self.heartbeat_thread.start()

    def log_tail(self):
        if self.log_file.is_file:
            with open(self.log_file, encoding="utf-8") as log_file:
                tail = ''.join(deque(log_file, 100))
        else:
            tail = ''

        return tail

    def heartbeat_send(self):
        try:
            self.server.post(f'/executor_api/jobs/{self.job_id}/heartbeat', json=dict(log_tail=self.log_tail()))
        except Exception as e:
            self.logger.error('Heartbeat exception: %s', e)

    def heartbeat(self):
        while not self.done.is_set():
            timer_or_event(self.cfg.heartbeat_interval, self.heartbeat_sleep)
            self.heartbeat_send()

    def download(self):

        self.job = self.server.get(f'/executor_api/jobs/{self.job_id}')
        self.heartbeat_send()
        job_files = self.server.get(f'/executor_api/jobs/{self.job_id}/files')
        job_packages = self.server.get(f'/executor_api/jobs/{self.job_id}/packages')

        (self.root / 'in').mkdir(parents=True, exist_ok=True)
        (self.root / 'in' / 'params.json').write_text(
                json.dumps(
                    {m['name'] : m['value'] for m in self.job['fields']},
                    ensure_ascii=False))

        files = {self.root / f['name']: (self.root, f)
                for f in job_files if Path(f['name']).parts[0] != 'out'}

        for package in job_packages:
            path = self.root / 'in' / str(package['id'])
            path.mkdir(parents=True, exist_ok=True)

            (path / 'label').write_text(package['label'])

            (path / 'fields.json').write_text(
                    json.dumps(
                        {m['name'] : m['value'] for m in package['fields']},
                        ensure_ascii=False))

        for p, f in files.values():
            self.server.download(f, folder=p)

        self.logger.info('Job inputs data downloaded.')

        self.server.post(f'/executor_api/jobs/{self.job_id}/status', json=dict(status='downloaded'))

    def execute(self):
        env = os.environ.copy()
        env.pop('RNDFLOW_REFRESH_TOKEN')

        base_url = os.environ.get('JUPYTER_BASE_URL')

        if self.job.get('is_interactive') and base_url:
            script = f"$jupyter_interactive --allow-root --no-browser --ip='*' --ServerApp.base_url={base_url} --IdentityProvider.token=''"
        else:
            script = self.job['node']['script'] or "echo 'Empty script: nothing to do :('\nexit 1"

        script_wrapper = dedent(f"""\
            if ! command -v ts; then
                echo "ts is not installed in the container!" > {self.job_id}.log
                exit 1
            fi
            if ! command -v tee; then
                echo "tee is not installed in the container!" > {self.job_id}.log
                exit 1
            fi

            if command -v jupyter-lab; then
                jupyter_interactive=jupyter-lab
            else
                jupyter_interactive=jupyter-notebook
            fi
            (
            {script}
            ) 2>&1 | ts "[{self.cfg.dateformat}]" | tee -a {self.job_id}.log
            rc=${{PIPESTATUS[0]}}
            exit $rc
            """)

        p = subprocess.run(script_wrapper, cwd=self.root, shell=True, executable="/bin/bash", check=False)
        self.status = p.returncode

    def upload(self):
        self.logger.info('Uploading job output to server and S3 server...')

        exclude_dirs = ('in', '__pycache__', '.ipynb_checkpoints')
        def enumerate_files():
            for directory, dirs, files in os.walk(self.root):
                path = Path(directory)
                dirs[:] = [d for d in dirs
                    if (path / d).relative_to(self.root).parts[0] not in exclude_dirs]
                for f in files:
                    if self.root / f != self.log_file:
                        yield path / f

        def upload_files(paths):

            def get_binary_and_type(path):
                binary = is_binary(str(path))

                file_type, _ = mimetypes.guess_type(str(path))
                if file_type is None:
                    file_type = 'application/x-binary' if binary else 'text/plain'

                return binary, file_type

            def upload_file_to_s3(link, path):
                if link is not None:
                    _, file_type = get_binary_and_type(path)
                    with open(path, 'rb') as f:
                        self.server.raw_session.put(link, data=f, headers={
                            'Content-Type': file_type,
                            'Content-Length': str(path.stat().st_size)
                            }).raise_for_status()

            p2h = {Path(path) : file_hash(path) for path in paths}

            h2p = {h : p for p,h in p2h.items()}
            links  = self.server.spec_post(f'/executor_api/jobs/{self.job_id}/upload_objects',
                    json={ 'objects': list(h2p.keys()) })

            self.logger.info('Uploading %s files to S3 server...', len(links))

            for item in links:
                path = h2p[item['object_id']]
                link = item['link']

                upload_file_to_s3(link, path)
                self.logger.info('Uploaded %s file to S3 server.', path)

            log_link = self.server.post(f'/executor_api/jobs/{self.job_id}/upload_objects', json={ 'objects': [file_hash(self.log_file)]})
            upload_file_to_s3(log_link[0]['link'], self.log_file)
            p2h[self.log_file] = file_hash(self.log_file)

            # Do not put any log output here! Log file size will be incorrect!

            files = []
            for path,h in p2h.items():
                binary, file_type = get_binary_and_type(path)
                files.append(dict(
                    name          = str(path.relative_to(self.root)),
                    type          = file_type,
                    content_hash  = h,
                    is_executable = os.access(path, os.X_OK),
                    is_binary     = binary,
                    size          = path.stat().st_size
                    ))

            return files

        files = upload_files(enumerate_files())

        self.logger.info('Uploading info to the server for creating output packages...')
        self.heartbeat_send()

        self.server.spec_put(f'/executor_api/jobs/{self.job_id}', json={
            'status': str(self.status),
            'files': files
            })

        self.data_upload = True

        self.logger.info('Jobs data uploading completed.')
        # self.heartbeat_send() # This heartbeat will be ignored by the server since the job will be proceed.

    def stop(self):
        self.done.set()
        self.heartbeat_sleep.set()
        self.heartbeat_thread.join()

    def __enter__(self):
        try:
            self.download()
            return self
        except Exception as e:
            self.stop()
            tr = traceback.format_exc()
            self.logger.error('Download error: %s', tr)
            self.server.post(f'/executor_api/jobs/{self.job_id}/error', json=dict(
                        error='DownloadError', message=tr))
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.upload()
        except Exception:
            tr = traceback.format_exc()
            self.logger.error('Upload error: %s', tr)
            con_tries = 0
            #If data was uploaded then ignore the error else try send info abotu error.
            while con_tries < 144 and not self.data_upload: # Try 24 hours: 60 min * 24 hour / 10 min = 144
                try:
                    self.server.post(f'/executor_api/jobs/{self.job_id}/error', json=dict(
                        error='UploadError', message=tr))
                    break
                except Exception as error:
                    self.logger.error('Post error exception: %s', {str(error)})
                    self.logger.error('Can not transfer error information to server. Wait...')
                    time.sleep(600)
                    con_tries +=1
        finally:
            self.stop()

#---------------------------------------------------------------------------
def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--host', dest='host', required=True)
    parser.add_argument('--job', dest='job', required=True, type=int)
    args = parser.parse_args()

    if 'RNDFLOW_REFRESH_TOKEN' not in os.environ:
        raise Exception('Access token not found in environment')

    with Job(args.host, args.job) as job:
        job.execute()
