file_readers = {}

#---------------------------------------------------------------------------
def load_hdf5(path):
    import h5py
    data = {}

    def load_dataset(name, obj):
        if not isinstance(obj, h5py.Dataset): return
        if name.startswith('_'): return

        path = name.split('/')
        dset = obj[()]   

        if len(path) == 1:
            data[name] = dset
        else:
            head, *path = path

            if head not in data:
                data[head] = {}

            g = data[head]

            while len(path) > 1:
                head, *path = path

                if head not in g:
                    g[head] = {}

                g = g[head]

            g[path[0]] = dset

    with h5py.File(path, 'r') as hdf:
        hdf.visititems(load_dataset)

    if len(data) == 1:
        data, = data.values()

    return data

#---------------------------------------------------------------------------
def load_json(path):
    import json
    from pathlib import Path
    return json.loads(Path(path).read_text())

#---------------------------------------------------------------------------
def load_csv(path):
    import pandas
    return pandas.read_csv(path)

#---------------------------------------------------------------------------
def register_file_reader(reader, *suffixes):
    for s in suffixes:
        file_readers[s] = reader

#---------------------------------------------------------------------------
register_file_reader(load_hdf5, '.h5', '.hdf5')
register_file_reader(load_json, '.json')
register_file_reader(load_csv, '.csv')
