import os
import io
import json
import pickle
import numpy as np
import pandas as pd
from aikit.tools.json_helper import SpecialJSONEncoder, SpecialJSONDecoder, save_json, load_json
from aikit.ml_machine.data_persister import SavingType, SharedInteger, Queue

from doppel.aws.s3 import S3Bucket


class MockLocker(object):
    def __init__(self):
        pass

    def __enter__(self):
        return None

    def __exit__(self, *args):
        pass


class S3DataPersister(object):

    def __init__(self, bucket_name, log_path=r'C:\data\logs', temp=r'C:\data\aikit\tmp.json'):
        self.bucket = S3Bucket(bucket_name)
        self.base_folder = log_path
        self.temp = temp
        self._cache = {}

    def get_complete_path(self, key, path, write_type):
        key = str(key)

        if path is None:
            complete_path = key + "." + write_type
        else:
            complete_path = self.bucket._validate_path(os.path.join(path, key + "." + write_type))

        return complete_path

    @classmethod
    def get_write_type(cls, write_type):

        if write_type is None:
            raise TypeError("write_type shouldn't be None")

        if not isinstance(write_type, str):
            raise TypeError("write_type should be a string, not a %s" % type(write_type))

        write_type = write_type.lower()
        if isinstance(write_type, str) and len(write_type) > 0 and write_type[0] == ".":
            write_type = write_type[1:]

        if write_type not in SavingType.alls:
            raise ValueError("I don't know how to handle that type of data : %s" % write_type)

        return write_type

    def get_lock(self, path, key, write_type):
        return MockLocker()

    def write(self, data, key, path=None, write_type=SavingType.pickle, _dont_lock=False):
        """ write a given key """
        write_type = self.get_write_type(write_type)
        complete_path = self.get_complete_path(key, path, write_type)

        if write_type == SavingType.json:
            save_json(data, self.temp)
            self.bucket.upload(self.temp, complete_path)
            #obj = json.dumps(data, indent=4, cls=SpecialJSONEncoder)
            #self.bucket.save(obj, complete_path)

        elif write_type == SavingType.csv:
            if isinstance(data, np.ndarray):
                data = pd.DataFrame(data)
            elif not isinstance(data, pd.DataFrame):
                raise TypeError("I don't know how to save this type %s to csv" % type(data))

            buffer = io.StringIO()
            data.to_csv(buffer, sep=";", encoding="utf-8", index=False)
            self.bucket.save(buffer, complete_path)

        elif write_type == SavingType.pickle:
            self.bucket.save_pickle(data, complete_path)

        elif write_type == SavingType.txt:
            self.bucket.save(data, complete_path)
        else:
            raise ValueError("Unknown writting type %s" % write_type)

    def read_from_cache(self, key, path=None, write_type=SavingType.pickle, _dont_lock=False):
        dico_key = (key, path, write_type)

        try:
            result = self._cache[dico_key]  #
        except KeyError:
            result = self.read(key=key, path=path, write_type=write_type, _dont_lock=_dont_lock)
            self._cache[dico_key] = result
        return result

    def read(self, key, path=None, write_type=SavingType.pickle, _dont_lock=False):
        """ read a given key """

        write_type = self.get_write_type(write_type)
        complete_path = self.get_complete_path(key, path, write_type)

        if not self.bucket.exists(complete_path):
            raise ValueError("The key %s doesn't exist in %s" % (key, path))

        if write_type == SavingType.json:
            with self.bucket.load(complete_path) as f:
                return json.load(f, cls=SpecialJSONDecoder)

        elif write_type == SavingType.pickle:
            data = self.bucket.load_pickle(complete_path)

        elif write_type == SavingType.csv:
            data = self.bucket.load(complete_path)
            data = pd.read_csv(data, sep=";", encoding="utf-8")

        elif write_type == SavingType.txt:
            data = self.bucket.load(complete_path)

        else:
            raise ValueError("Unknown writting type %s" % write_type)

        return data

    def exists(self, key, path=None, write_type=SavingType.pickle):
        write_type = self.get_write_type(write_type)
        complete_path = self.get_complete_path(key, path, write_type)
        return self.bucket.exists(complete_path)

    def delete(self, key, path=None, write_type=SavingType.pickle):
        write_type = self.get_write_type(write_type)
        complete_path = self.get_complete_path(key, path, write_type)
        self.bucket.remove(complete_path)

    def alls(self, path=None, write_type=SavingType.pickle):
        write_type = self.get_write_type(write_type)
        complete_path = self.get_complete_path(key="*", path=path, write_type=write_type)
        folders, files = self.bucket.listdir(path)
        keys = [os.path.splitext(f)[0] for f in files]
        return keys

    def new_shared_integer(self, path, key):
        return SharedInteger(data_persister=self, path=path, key=key)

    def new_queue(self, path, write_type=SavingType.json, max_queue_size=None, random=False):
        """ create a new queue """
        return Queue(
            data_persistor=self, path=path, write_type=write_type, max_queue_size=max_queue_size, random=random
        )

    def add_in_queue(self, data, path, write_type=SavingType.pickle, max_queue_size=None):
        all_items = self.alls(path, write_type=write_type)
        if max_queue_size is not None and len(all_items) >= max_queue_size:
            return False

        if len(all_items) > 0:
            key = max([int(i) for i in all_items]) + 1
        else:
            key = 0

        self.write(data=data, key=key, path=path, write_type=write_type)
        return True

    def remove_from_queue(self, path, write_type=SavingType.pickle, random=False):
        all_items = sorted(self.alls(path, write_type=write_type), key=lambda x: int(x))
        if len(all_items) == 0:
            data = None
        else:
            if random:
                choice = np.random.choice(list(range(len(all_items))), 1)[0]
            else:
                choice = 0

            key = all_items[choice]
            data = self.read(key=key, path=path, write_type=write_type)
            self.delete(key=key, path=path, write_type=write_type)
        return data
