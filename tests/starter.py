import os
import time
from doppel.aws.s3 import S3Client
from doppel import DoppelContext, DoppelProject, destroy_all_projects

from tests.utils import console_logger, get_root_path


context = DoppelContext() \
    .add_data(key='titanic.csv', bucket='doppel-project-data', source=r'C:\data\titanic\train.csv') \
    .add_data(key='titanic_test.csv', bucket='doppel-project-data', source=r'C:\data\titanic\test.csv')
context.upload_data()

ROOT_PATH = get_root_path()


def run_starter():
    project = DoppelProject(
        'starter',
        path=os.path.join(ROOT_PATH, 'examples', 'starter.py'),
        packages=[ROOT_PATH],
        n_instances=1,
        min_memory=4, min_cpu=1,
        context=context
    )
    project.start()
    assert project.bucket.exists('doppel.config')
    assert project.bucket.exists('doppel.status')

    status = project.bucket.load_json('doppel.status')
    assert status['status'] == 'running'

    time.sleep(10)
    
    project.terminate()
    status = project.bucket.load_json('doppel.status')
    assert status['status'] == 'terminated'

    project.destroy()
    assert not S3Client().bucket_exists(project.arn)


if __name__ == '__main__':
    console_logger()
    destroy_all_projects()
    run_starter()
