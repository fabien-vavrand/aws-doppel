from doppel.aws.s3 import S3Client, S3Bucket
from doppel.core.project import DoppelProject


def iterate_projects():
    s3 = S3Client()
    buckets = s3.list_buckets()
    for bucket in buckets:
        bucket_name = bucket['Name']
        if bucket_name.startswith('doppel-'):
            bucket = S3Bucket(bucket_name)
            if bucket.exists('doppel.status'):
                status = bucket.load_json('doppel.status')
                yield bucket_name, status


def list_projects():
    for bucket_name, status in iterate_projects():
        print('{}: {}'.format(bucket_name, status))


def destroy_all_projects():
    for bucket_name, status in iterate_projects():
        project = DoppelProject.load(status['name'])
        project.destroy()
