from aikit.ml_machine.data_persister import FolderDataPersister
from doppel import DoppelContext
from doppelkit.persister import S3DataPersister
from doppelkit.launcher import MlMachineLauncher

context = DoppelContext()
context.get_logger()

#persister = FolderDataPersister(r'C:\data\aikit')
persister = S3DataPersister('aikit-automl-run', log_path=r'/home/ec2-user/doppel/ailogs', temp=r'/home/ec2-user/doppel/tmp.json')
launcher = MlMachineLauncher(persister=persister)
launcher.reload()
worker = launcher.create_worker()
worker.run()
