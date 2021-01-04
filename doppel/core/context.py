import os
import io
import sys
import json
import pickle
import zipfile
import logging
from typing import Dict, Optional
from doppel.aws.s3 import S3Bucket


class DoppelContext:

    def __init__(self, data: Optional[Dict[str, Dict[str, str]]] = None):
        self.data = data if data is not None else {}
        self.is_doppel = os.getenv('DOPPEL') is not None
        self.doppel_name = os.getenv('DOPPEL_NAME')
        self.doppel_arn = os.getenv('DOPPEL_ARN')
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

    def data_path(self, key=None):
        if self.is_doppel:
            path = '/home/ec2-user/doppel/data'
            if key:
                return '{}/{}'.format(path, key)
            else:
                return path
        else:
            if key is None:
                raise ValueError('key should not be null when running locally')
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

    def save(self, obj, doppel_path, local_path=None):
        if self.is_doppel:
            S3Bucket(self.doppel_arn).save(obj, doppel_path)
        elif local_path:
            with open(local_path, 'wb') as file:
                file.write(obj.getvalue())

    def save_json(self, obj, doppel_path, local_path=None):
        if self.is_doppel:
            S3Bucket(self.doppel_arn).save_json(obj, doppel_path)
        elif local_path:
            with open(local_path, 'w') as file:
                json.dump(obj, file, indent=4)

    def save_pickle(self, obj, doppel_path, local_path=None, zip=False):
        if self.is_doppel:
            S3Bucket(self.doppel_arn).save_pickle(obj, doppel_path, zip=zip)
        elif local_path:
            buffer = io.BytesIO()
            if not zip:
                buffer = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip:
                    zip.writestr('object.pkl', pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))
            with open(local_path, 'wb') as file:
                file.write(buffer.getvalue())

    def get_logger(self, name=None):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        if self.is_doppel:
            handler = logging.FileHandler('/home/ec2-user/doppel/logs')
        else:
            handler = logging.StreamHandler(stream=sys.stdout)

        LOGGING_FORMAT = '%(asctime)-15s %(name)-15s %(levelname)-8s %(message)s'
        DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
        handler.setFormatter(logging.Formatter(fmt=LOGGING_FORMAT, datefmt=DATE_FORMAT))
        logger.addHandler(handler)
        return logger
