import os
from doppel.aws.s3 import S3Client
from doppel import DoppelProject, destroy_all_projects

from tests.utils import console_logger, get_root_path


ROOT_PATH = get_root_path()


def run_search():
    project = DoppelProject(
        'search',
        path=os.path.join(ROOT_PATH, 'examples', 'search.py'),
        requirements=['scikit-learn'],
        packages=[ROOT_PATH],
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