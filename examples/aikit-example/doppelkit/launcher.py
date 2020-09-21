from aikit.ml_machine.data_persister import SavingType
from aikit.ml_machine import AutoMlConfig, JobConfig, AutoMlResultReader, AutoMlModelGuider, MlJobRunner, MlJobManager


class MlMachineLauncher(object):

    def __init__(self, persister, name=None, loader=None, set_configs=None):
        self.persister = persister
        self.name = name
        self.loader = loader
        self.set_configs = set_configs

        self.job_config = None
        self.auto_ml_config = None

        self.dfX = None
        self.y = None
        self.groups = None

        self._seed = None  # to initialize the attribute

    def initialize(self):
        """ method to initialize auto_ml_config and job_config """

        if self.dfX is None or self.y is None:
            temp = self.loader()
            if len(temp) == 2:
                self.dfX, self.y = temp
                self.groups = None
            else:
                self.dfX, self.y, self.groups = temp

        if self.auto_ml_config is None:
            self.auto_ml_config = AutoMlConfig(dfX=self.dfX, y=self.y, groups=self.groups, name=self.name)
            self.auto_ml_config.guess_everything()

        if self.job_config is None:
            self.job_config = JobConfig()
            self.job_config.guess_cv(auto_ml_config=self.auto_ml_config, n_splits=10)
            self.job_config.guess_scoring(auto_ml_config=self.auto_ml_config)

        if self.set_configs is not None:
            self.set_configs(self)

    def persist(self):
        """ method to persist 'auto_ml_config', 'job_config', 'dfX' and 'y' """

        self.auto_ml_config.dfX = None
        self.auto_ml_config.y = None
        self.auto_ml_config.groups = None

        self.persister.write(data=self.job_config, key="job_config", write_type=SavingType.pickle)
        self.persister.write(data=self.auto_ml_config, key="auto_ml_config", write_type=SavingType.pickle)

        self.persister.write(data=self.dfX, key="dfX", write_type=SavingType.pickle)
        self.persister.write(data=self.y, key="y", write_type=SavingType.pickle)
        self.persister.write(data=self.groups, key="groups", write_type=SavingType.pickle)

    def reload(self):
        """ method to reload dfX, y, auto_ml_config and job_config """

        self.job_config = self.persister.read(key="job_config", write_type=SavingType.pickle)
        self.auto_ml_config = self.persister.read(key="auto_ml_config", write_type=SavingType.pickle)

        self.dfX = self.persister.read(key="dfX", write_type=SavingType.pickle)
        self.y = self.persister.read(key="y", write_type=SavingType.pickle)
        self.groups = self.persister.read(key="groups", write_type=SavingType.pickle)

    def create_controller(self):
        """ create a controller object, if it doesn't exist, but doesn't start it """

        result_reader = AutoMlResultReader(self.persister)
        auto_ml_guider = AutoMlModelGuider(
            result_reader=result_reader,
            job_config=self.job_config,
            metric_transformation="default",
            avg_metric=True,
        )
        job_controller = MlJobManager(
            auto_ml_config=self.auto_ml_config,
            job_config=self.job_config,
            auto_ml_guider=auto_ml_guider,
            data_persister=self.persister,
            seed=self._seed,
        )
        return job_controller

    def create_worker(self):
        """ create a worker, if it doesn't exist, but doesn't start it """
        job_runner = MlJobRunner(
            dfX=self.dfX,
            y=self.y,
            groups=self.groups,
            auto_ml_config=self.auto_ml_config,
            job_config=self.job_config,
            data_persister=self.persister,
            seed=self._seed,
        )
        return job_runner
