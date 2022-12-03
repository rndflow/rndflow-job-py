import contextlib
import json
import mimetypes
import os
import subprocess
import sys
import threading
import time
import traceback

from binaryornot.check import is_binary
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from .server import Server, file_hash, timestamp

class Job:
    HEARTBEAT_INTERVAL = timedelta(seconds=60)

    def __init__(self, host: str, job_id: int):
        self.job_id = job_id
        self.server = Server(host)

        self.root = Path(str(job_id)).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

        self.log_file = self.root / f'{self.job_id}.log'
        self.done = threading.Event()
        self.beat = threading.Thread(target=self.heartbeat)
        self.beat.start()

    def heartbeat(self):
        def send():
            try:
                if self.log_file.is_file:
                    tail = ''.join(deque(open(self.log_file), 100))
                else:
                    tail = ''

                self.server.post(f'/executor_api/jobs/{self.job_id}/heartbeat',
                        json=dict(log_tail=tail))
            except:
                print(traceback.format_exc())

        chkpt = None
        while not self.done.is_set():
            time.sleep(0.5)
            if chkpt is None or datetime.utcnow() >= chkpt:
                send()
                chkpt = datetime.utcnow() + self.HEARTBEAT_INTERVAL

    def download(self):
        with open(self.log_file, 'at', buffering=1) as log_file:
            with contextlib.redirect_stdout(log_file):
                self.job = self.server.get(f'/executor_api/jobs/{self.job_id}')
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

                print(f'[{timestamp()}] Job inputs downloaded')

    def execute(self):
        env = os.environ.copy()
        env.pop('RNDFLOW_REFRESH_TOKEN')

        base_url = os.environ.get('JUPYTER_BASE_URL')

        if self.job.get('is_interactive') and base_url:
            script = f"$jupyter_interactive --allow-root --no-browser --ip='*' --NotebookApp.base_url={base_url} --NotebookApp.token=''"
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
            ) 2>&1 | ts "[%Y-%m-%d %H:%M:%S]" | tee -a {self.job_id}.log
            rc=${{PIPESTATUS[0]}}
            exit $rc
            """)

        p = subprocess.run(script_wrapper, cwd=self.root, shell=True, executable="/bin/bash")
        self.status = p.returncode

    def upload(self):
        with open(self.log_file, 'at', buffering=1) as log_file:
            with contextlib.redirect_stdout(log_file):
                print(f'[{timestamp()}] Uploading job output to server...')

                exclude_dirs = ('in', '__pycache__', '.ipynb_checkpoints')
                def enumerate_files():
                    for dir, dirs, files in os.walk(self.root):
                        path = Path(dir)
                        dirs[:] = [d for d in dirs
                            if (path / d).relative_to(self.root).parts[0] not in exclude_dirs]
                        for f in files:
                            yield path / f

                def upload_files(paths):
                    p2h = {Path(path) : file_hash(path) for path in paths}

                    h2p = {h : p for p,h in p2h.items()}
                    links  = self.server.post(f'/executor_api/jobs/{self.job_id}/upload_objects',
                            json={ 'objects': list(h2p.keys()) })

                    print(f'[{timestamp()}] Uploading {len(links)} files to server...')

                    for item in links:
                        path = h2p[item['object_id']]
                        link = item['link']

                        binary = is_binary(str(path))
                        type,_ = mimetypes.guess_type(str(path))
                        if type is None:
                            type = 'application/x-binary' if binary else 'text/plain'

                        if link is not None:
                            with open(path, 'rb') as f:
                                self.server.raw_session.put(link, data=f, headers={
                                    'Content-Type': type,
                                    'Content-Length': str(path.stat().st_size)
                                    }).raise_for_status()


                    files = []
                    for path,h in p2h.items():
                        binary = is_binary(str(path))
                        type,_ = mimetypes.guess_type(str(path))
                        if type is None:
                            type = 'application/x-binary' if binary else 'text/plain'

                        files.append(dict(
                            name          = str(path.relative_to(self.root)),
                            type          = type,
                            content_hash  = h,
                            is_executable = os.access(path, os.X_OK),
                            is_binary     = binary,
                            size          = path.stat().st_size
                            ))

                    return files

                files = upload_files(enumerate_files())

                print(f'[{timestamp()}] Uploading status info to server...')

                self.server.put(f'/executor_api/jobs/{self.job_id}', json={
                    'status': str(self.status),
                    'files': files
                    })


    def __enter__(self):
        self.download()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.upload()
        except Exception as e:
            tr = traceback.format_exc()
            print(tr)
            conTries = 0
            while conTries < 288: # Try 2 days: 60 min * 24 hour * 2 days / 10 min = 288
                try:
                    self.server.post(f'/executor_api/jobs/{self.job_id}/error', json=dict(
                        error='UploadError', message=tr))
                    break
                except Exception as re:
                    tr = traceback.format_exc()
                    print(tr)
                    print('Can not transfer error information to server. Wait...')
                    time.sleep(600)
                    conTries +=1

        finally:
            self.done.set()
            self.beat.join()


#---------------------------------------------------------------------------
def main():
    import argparse
    from getpass import getpass

    parser = argparse.ArgumentParser()
    parser.add_argument('--host', dest='host', required=True)
    parser.add_argument('--job', dest='job', required=True, type=int)
    args = parser.parse_args()

    if 'RNDFLOW_REFRESH_TOKEN' not in os.environ:
        raise Exception('Access token not found in environment')

    with Job(args.host, args.job) as job:
        job.execute()
