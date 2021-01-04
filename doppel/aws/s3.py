import io
import json
import boto3
import pickle
import zipfile
import logging
import datetime
import threading

from boto3.s3.transfer import TransferConfig, MB
from doppel.aws.__init__ import AwsClient


class S3Client(AwsClient):

    def __init__(self, profile_name=None):
        super().__init__('s3', profile_name)

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

    def create_presigned_url(self, bucket_name, object_name, expiration=300):
        """Generate a presigned URL to share an S3 object

        :param bucket_name: string
        :param object_name: string
        :param expiration: Time in seconds for the presigned URL to remain valid
        :return: Presigned URL as string. If error, returns None.
        """

        # Generate a presigned URL for the S3 object
        try:
            response = self.client.generate_presigned_url(
                'get_object', Params={'Bucket': bucket_name, 'Key': object_name}, ExpiresIn=expiration)
        except Exception as e:
            logging.error(e)
            return None

        return response


class S3Bucket(AwsClient):

    def __init__(self, name):
        super().__init__('s3')
        self.name = name
        self.bucket = None
        self.init_bucket()

    def init_bucket(self):
        self.bucket = self.resource.Bucket(self.name)
        if self.bucket.creation_date is None:
            if self.region is None:
                raise ValueError('Bucket does not exist. Define a region to be able to create one.')
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

    def save_pickle(self, obj, path, zip=False):
        buffer = io.BytesIO()
        if not zip:
            pickle.dump(obj, buffer, protocol=pickle.HIGHEST_PROTOCOL)
        else:
            with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip:
                zip.writestr('object.pkl', pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))
        self.save(buffer, path)

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

    def load_pickle(self, path, zip=False):
        buffer = self.load(path)
        if not zip:
            return pickle.loads(buffer.getvalue())
        else:
            with zipfile.ZipFile(buffer, 'r') as zip:
                return pickle.loads(zip.read('object.pkl'))

    def walk(self, path):
        raise NotImplementedError()

    def remove(self, path):
        path = self._validate_path(path)
        if self.exists(path):
            self.bucket.delete_objects(Delete={'Objects': [{'Key': path}]})

    def save_multiparts(self, buffer, path):
        buffer.seek(0)
        path = self._validate_path(path)
        config = TransferConfig(
            multipart_threshold=100 * MB,
            multipart_chunksize=100 * MB,
            max_concurrency=10,
            use_threads=True
        )
        self.client.upload_fileobj(
            Fileobj=buffer,
            Bucket=self.name,
            Key=path,
            Config=config
            #Callback=ProgressPercentage(path, size=buffer.__sizeof__())
        )

    def save_pickle_multiparts(self, obj, path):
        buffer = io.BytesIO()
        pickle.dump(obj, buffer, protocol=pickle.HIGHEST_PROTOCOL)
        self.save_multiparts(buffer, path)


class ProgressPercentage(object):

    def __init__(self, name, size=None):
        self.name = name
        self.size = size
        self.uploaded = 0
        self.percent = 0
        self.lock = threading.Lock()
        self.logger = logging.getLogger('aws-upload')

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self.lock:
            self.uploaded += bytes_amount
            if self.size is None:
                self.logger.info('Uploaded {} bytes of {}'.format(self.uploaded, self.name))
            else:
                percent = int(self.uploaded / self.size * 100)
                if percent > self.percent:
                    self.percent = percent
                    self.logger.info('Uploaded {}% of {}'.format(self.percent, self.name))