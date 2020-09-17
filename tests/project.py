import os
from doppel.aws.s3 import S3Client
from doppel.core.context import DoppelContext
from doppel.core.project import DoppelProject
from doppel.core.management import destroy_all_projects

from tests.utils import console_logger


context = DoppelContext() \
    .add_data(key='titanic.csv', bucket='doppel-project-data', source=r'C:\data\titanic\train.csv') \
    .add_data(key='titanic_test.csv', bucket='doppel-project-data', source=r'C:\data\titanic\test.csv')
context.upload_data()

DOPPEL_PATH = os.environ['DOPPEL_PATH']


def run_starter():
    project = DoppelProject(
        'test-project',
        path=DOPPEL_PATH,
        entry_point='-m doppel.examples.starter',
        n_instances=2, min_memory=4, min_cpu=1,
        data=context
    )
    project.start()
    assert project.bucket.exists('doppel.config')
    assert project.bucket.exists('doppel.status')

    status = project.bucket.load_json('doppel.status')
    assert status['status'] == 'running'

    project.terminate()
    status = project.bucket.load_json('doppel.status')
    assert status['status'] == 'terminated'

    project.destroy()
    assert not S3Client().bucket_exists(project.arn)


def run_search():
    project = DoppelProject(
        'search',
        path=DOPPEL_PATH,
        entry_point='-m doppel.examples.search',
        n_instances=10, duration=0.1,
        min_memory=4, min_cpu=1
    )
    project.start()
    assert project.bucket.exists('doppel.config')
    assert project.bucket.exists('doppel.status')

    status = project.bucket.load_json('doppel.status')
    assert status['status'] == 'running'

    project.monitore()
    status = project.bucket.load_json('doppel.status')
    assert status['status'] == 'terminated'

    _, files = project.bucket.listdir('results')
    print('{} searches performed'.format(len(files)))
    assert len(files) > 20

    project.destroy()
    assert not S3Client().bucket_exists(project.arn)


if __name__ == '__main__':
    console_logger()
    destroy_all_projects()
    run_search()
