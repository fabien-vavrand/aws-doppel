import io
import boto3
import logging
import datetime
import json
from doppel.aws import AwsClient


class S3Client(AwsClient):

    def __init__(self):
        super().__init__('s3')

    def list_buckets(self):
        buckets = self.client.list_buckets()
        return buckets['Buckets']

    def delete_buckets(self):
        for bucket in self.list_buckets():
            self.resource.Bucket(bucket['Name']).delete()

    def bucket_exists(self, name):
        try:
            self.client.head_bucket(Bucket=name)
            return True
        except Exception as e:
            # Does not exists or don't have access
            return False


class S3Bucket(AwsClient):

    def __init__(self, name):
        super().__init__('s3')
        self.name = name
        self.bucket = None
        self.init_bucket()

    def init_bucket(self):
        self.bucket = self.resource.Bucket(self.name)
        if self.bucket.creation_date is None:
            self.logger.info('Creating bucket {}'.format(self.name))
            self.bucket = self.resource.create_bucket(Bucket=self.name,
                                                      CreateBucketConfiguration={'LocationConstraint': self.region})

    def block_public_access(self):
        return self.client.put_public_access_block(
            Bucket=self.name,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True
            }
        )

    def empty(self):
        for key in self.bucket.objects.all():
            key.delete()

    def delete(self):
        self.bucket.delete()
        self.bucket = None

    def _validate_path(self, path):
        if path is None:
            path = ''
        elif isinstance(path, list):
            path = '/'.join(path)
        path = path.replace('\\', '/')
        path = path.strip('/')
        return path

    def exists(self, path):
        path = self._validate_path(path)
        objs = list(self.bucket.objects.filter(Prefix=path))
        if len(objs) > 0 and objs[0].key == path:
            return True
        else:
            return False

    def has_folder(self, path):
        path = self._validate_path(path)
        if path == '':
            raise ValueError('folder needed')
        path += '/'
        objs = list(self.bucket.objects.filter(Prefix=path))
        return len(objs) > 0

    def iterate(self, path=None):
        path = self._validate_path(path)
        if path != '':
            path += '/'

        s3_paginator = self.client.get_paginator('list_objects_v2')
        for page in s3_paginator.paginate(Bucket=self.name, Prefix=path):
            for content in page.get('Contents', ()):
                yield content['Key']

    def listdir(self, path=None):
        path = self._validate_path(path)
        if path != '':
            path += '/'
        files = list(self.iterate(path))
        files = [f[len(path):] for f in files]

        folders = list(set([f.split('/')[0] for f in files if '/' in f]))
        files = [f for f in files if '/' not in f]
        return folders, files

    def save(self, obj, path):
        path = self._validate_path(path)
        if isinstance(obj, dict) or isinstance(obj, list):
            self.save_json(obj, path)
        elif isinstance(obj, io.StringIO) or isinstance(obj, io.BytesIO):
            self.bucket.put_object(Key=path, Body=obj.getvalue())
        else:
            self.bucket.put_object(Key=path, Body=obj)

    def save_json(self, obj, path):
        def default(obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()

        obj = json.dumps(obj, default=default, indent=4)
        self.save(obj, path)

    def upload(self, filename, path):
        path = self._validate_path(path)
        self.bucket.upload_file(filename, path)

    def load(self, path):
        path = self._validate_path(path)
        if not self.exists(path):
            return FileNotFoundError('file {} does not exists in the bucket'.format(path))
        obj = self.bucket.Object(path)
        return io.BytesIO(obj.get()['Body'].read())

    def load_json(self, path):
        with self.load(path) as f:
            return json.load(f)

    def walk(self, path):
        raise NotImplementedError()
