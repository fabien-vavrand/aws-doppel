import os
import sys
import json
import logging
from typing import Dict, Optional
from doppel.aws.s3 import S3Bucket


class DoppelContext:

    def __init__(self, data: Optional[Dict[str, Dict[str, str]]] = None):
        self.data = data if data is not None else {}
        self.is_doppel = os.getenv('DOPPEL') is not None
        self.doppel_name = os.getenv('DOPPEL_NAME')
        self._validate()

    def _validate(self):
        for key, ds in self.data.items():
            if 'bucket' not in ds:
                raise ValueError('bucket in missing from data {}'.format(key))

    def add_data(self, key, bucket, source=None):
        data = {
            'bucket': bucket
        }
        if source is not None:
            data['source'] = source

        self.data[key] = data
        return self

    def data_path(self, key):
        if self.is_doppel:
            return os.path.join('/home/ec2-user/doppel/data/{}'.format(key))
        else:
            if key not in self.data:
                raise ValueError('key {} is missing in data'.format(key))
            if 'source' not in self.data[key]:
                raise ValueError('source is not defined in data {}'.format(key))
            return self.data[key]['source']

    def upload_data(self, force=False):
        for key, data in self.data.items():
            if 'source' in data:
                bucket = S3Bucket(data['bucket'])
                if force or not bucket.exists(key):
                    logging.info('Uploading {} to {}'.format(key, data['bucket']))
                    bucket.upload(data['source'], key)

    def save_json(self, obj, doppel_path, local_path):
        if self.is_doppel:
            S3Bucket(self.doppel_name).save_json(obj, doppel_path)
        else:
            with open(local_path, 'w') as file:
                json.dump(obj, file, indent=4)

    def save_pickle(self, obj, doppel_path, local_path):
        raise NotImplementedError()

    def get_logger(self, name=None):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        if self.is_doppel:
            handler = logging.FileHandler('/home/ec2-user/doppel/logs')
        else:
            handler = logging.StreamHandler(stream=sys.stdout)

        logger.addHandler(handler)
        return logger
