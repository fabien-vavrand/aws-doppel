from aikit.datasets import load_dataset, DatasetEnum
from aikit.ml_machine.data_persister import FolderDataPersister
from doppel import DoppelContext
from doppelkit.persister import S3DataPersister
from doppelkit.launcher import MlMachineLauncher


def loader():
    dfX, y, *_ = load_dataset(DatasetEnum.titanic)
    return dfX, y


def set_configs(launcher):
    launcher.job_config.allow_approx_cv = True
    return launcher


context = DoppelContext()
context.get_logger()

#persister = FolderDataPersister(r'C:\data\aikit')
persister = S3DataPersister('aikit-automl-run', log_path=r'/home/ec2-user/doppel/ailogs', temp=r'/home/ec2-user/doppel/tmp.json')
launcher = MlMachineLauncher(
    persister=persister,
    name="titanic",
    loader=loader,
    set_configs=set_configs
)
launcher.initialize()
launcher.persist()
controler = launcher.create_controller()
controler.run()
