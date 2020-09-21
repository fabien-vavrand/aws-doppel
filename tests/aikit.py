import os
from doppel import DoppelProject, destroy_all_projects

from tests.utils import console_logger, get_root_path


ROOT_PATH = get_root_path()


def run_aikit():
    controller = DoppelProject(
        'aikit-controller',
        path=os.path.join(ROOT_PATH, 'examples', 'aikit-example'),
        entry_point='doppelkit/run_controller.py',
        requirements=['aikit'],
        packages=[ROOT_PATH],
        n_instances=1, min_memory=2, min_cpu=1,
        commands=['mkdir -p doppel/ailogs/mljobmanager_workers']
    )
    controller.start()

    workers = DoppelProject(
        'aikit-worker',
        path=os.path.join(ROOT_PATH, 'examples', 'aikit-example'),
        entry_point='doppelkit/run_worker.py',
        requirements=['aikit'],
        packages=[ROOT_PATH],
        n_instances=20, duration=0.2,
        min_memory=2, min_cpu=16,
        commands=['mkdir -p doppel/ailogs/mljobrunner_workers']
    )
    workers.start()
    workers.monitore()

    controller.terminate()


if __name__ == '__main__':
    console_logger()
    destroy_all_projects()
    run_aikit()
