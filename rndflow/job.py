import os
import json
from pathlib import Path

from .file_readers import file_readers

root = Path('.').resolve()
job_id = int(root.name)
package_index = 0

#---------------------------------------------------------------------------
def secret(name, default=None):
    vname = f'RNDFLOW_SECRET_{name.upper()}'
    value = os.environ.get(vname)
    if default is None and value is None:
        raise Exception(f'The required environment variable "{vname}" is not set')
    return default if value is None else value

#---------------------------------------------------------------------------
class Package:
    def __init__(self, path):
        self.path = path

    @property
    def id(self):
        return int(self.path.name)

    @property
    def label(self):
        path = self.path / 'label'
        if path.is_file():
            return path.read_text().strip()

    def files(self, *suffixes):
        return [f for f in (self.path / 'files').glob('*')
                if f.is_file() and (not suffixes or f.suffix.lower() in suffixes) ]

    @property
    def fields(self):
        path = self.path / 'fields.json'
        if path.is_file():
            return json.loads(path.read_text())
        return {}

    def load(self, readers={}):
        data = self.fields
        readers = {**file_readers, **readers}

        for f in self.files():
            reader = readers.get(f.suffix.lower())
            if reader:
                data[f.stem] = reader(f)
            else:
                print(f'Skipping {f}: unknown format')

        return data

#---------------------------------------------------------------------------
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        import numpy as np

        if isinstance(obj, np.generic):
            return obj.item()

        return super(NumpyEncoder, self).default(obj)

#---------------------------------------------------------------------------
def params():
    path = root / 'in' / 'params.json'
    if path.is_file():
        return json.loads(path.read_text())
    return {}

#---------------------------------------------------------------------------
def packages():
    return [Package(p) for p in (root / 'in').glob('*') if p.is_dir()]

#---------------------------------------------------------------------------
def files(*suffixes):
    for p in packages():
        for f in p.files(*suffixes):
            yield f

#---------------------------------------------------------------------------
def load(readers={}):
    data = params()

    for p in packages():
        data.update(p.load(readers))

    return data

#---------------------------------------------------------------------------
def save_hdf5(path, data):
    import h5py
    import numpy

    with h5py.File(path, 'w') as f:
        f.create_dataset(path.stem, data=data, track_times=False,
                compression='gzip' if isinstance(data, numpy.ndarray) else None)

#---------------------------------------------------------------------------
def save_package(label=None, files={}, fields={}, images={}):
    global package_index

    package_index += 1
    path = root / 'out' / str(package_index)
    path.mkdir(parents=True, exist_ok=True)

    if label is not None:
        (path / 'label').write_text(str(label).strip())

    if fields:
        if not isinstance(fields, dict):
            raise ValueError('fields parameter should be a dictionary')

        (path / 'fields.json').write_text(json.dumps(fields, ensure_ascii=False, cls=NumpyEncoder))

    if files or images:
        (path / 'files').mkdir(parents=True, exist_ok=True)

    for k,v in files.items():
        f = path / 'files' / k

        if callable(v):
            v(f)
        else:
            save_hdf5(f.with_suffix('.h5'), v)

    for k,v in images.items():
        f = path / 'files' / k
        if callable(v):
            v(f)
        elif v.__module__.startswith('matplotlib'):
            if not f.suffix:
                f = f.with_suffix('.png')
            v.savefig(f, transparent=True)
        elif v.__module__.startswith('plotly'):
            v.write_json(str(f.with_suffix('.plt')))

        else:
            print(f'Skipped image {k}: unknown format')

    return path
