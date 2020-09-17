import os
import time
import logging
import threading
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict

from doppel.aws.ec2 import Ec2Client, Ec2
from doppel.aws.s3 import S3Bucket, S3Client
from doppel.aws.iam import IamClient, Policy
from doppel.ssh import SshSession
from doppel.utils import zip_dir, get_root_path
from doppel.core.context import DoppelContext


logging.getLogger('botocore.credentials').setLevel(logging.WARNING)
logging.getLogger('retry.api').setLevel(logging.ERROR)
logging.getLogger('paramiko.transport').setLevel(logging.WARNING)
logger = logging.getLogger('doppel')

KEY = 'doppel'


class DoppelProject:
    """
    Object allowing to create and manage a computation project deployed on AWS EC2 instances.
    Once initialized, the project creates a dedicated bucket on AWS S3.

    Parameters
    ----------
    name : string
        The project name, used to name the S3 bucket.

    src : string, default=None
        Python code snippet to directly execute on EC2 instances. Cannot be used with path.

    path : string, default=None
        Path to a python file or a project directory, to run on EC2 instances. Cannot be used with src.

    entry_point : string, default=None
        Python entry point to execute when path is passed as a directory. If path points to a python project with a
        setup.py, the entry point should be of the form -m module.module, instead of module/module.py

    dependencies : list of string, default=None
        List of packages dependencies to install on EC2 instances prior to running the code. Should not be passed when
        path is a project with a setup.py (and a requirements.txt).

    env_vars : List of string, default=None
        List of environment variables to set on EC2 instances.

    python : string, default=None
        Python version to use when creating virtual environment on EC2 instances. When None, the latest version is used.

    n_instances : integer, default=None
        Number of instances to start on AWS. When None, calculated using duration and budget if possible, else default
        to 1.

    duration : float, default=None
        Duration (in hours) during which instances should run. When None, calculated using n_instances and budget if
        possible, else run indefinitly.

    budget : float, default=None
        Budget (in your AWS profile currency) allocated to the project. When None, calculated using n_instances and
        duration if possible, else no budget limit is defined.

    min_memory : float, default=None
        Minimum RAM memory, in Gb, for the EC2 instances to have. If not defined, no lower limit is applied.

    min_cpu : integer, default=None
        Minimum number of vCPUs for the EC2 instances to have. If not defined, no lower limit is applied.

    min_gpu : integer, default=None
        Minimum number of GPUs for the EC2 instance to have. If not defined, no lower limit is applied.

    context : DoppelContext, default=None
        When starting new instances, the project will copy data defined on the context to each instance. Those data
        can then be accessed using context.data_path(<key>), which will return the local path when the code is running
        locally, or the remote path when the code is running on AWS.

    key_path : string, default=None
        Path to an AWS key pair pem file to use instead of creating a new key pair. The file should be of the form
        <key_pair.pem>, where <key_pair> is the name of the key pair already existing on AWS.

    _from_config : boolean, default=False
        Used internally


    Examples
    --------
    >>> context = DoppelContext() \
    >>>     .add_data(key='train', bucket='my-project-data', source=r'C:\data\project\train.csv') \
    >>>     .add_data(key='test', bucket='my-project-data', source=r'C:\data\project\test.csv')
    >>> context.upload_data()
    >>>
    >>> project = DoppelProject(
    >>>     name='project-run-1',
    >>>     path=r'C:\app\project',
    >>>     entry_point='-m project.run',
    >>>     n_instances=10, budget=20,
    >>>     min_memory=16, min_cpu=8,
    >>>     context=context
    >>> )
    >>> project.start()
    >>> project.monitore()
    """

    def __init__(
        self,
        name: str,
        src: Optional[str] = None,
        path: Optional[str] = None,
        entry_point: Optional[str] = None,
        dependencies: Optional[List[str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        python: Optional[str] = None,
        n_instances: Optional[int] = None,
        duration: Optional[float] = None,
        budget: Optional[float] = None,
        min_memory: Optional[float] = None,
        min_cpu: Optional[int] = None,
        min_gpu: Optional[int] = None,
        context: Optional[DoppelContext] = None,
        key_path: Optional[str] = None,
        _from_config: bool = False
    ):
        self.functional_name = name
        self.name = self._format_name(name)
        self.arn = self._get_arn(name)
        self.src = src
        self.path = path
        self.entry_point = entry_point
        self.dependencies = dependencies
        self.env_vars = env_vars
        self.python = python
        self.n_instances = n_instances
        self.duration = duration
        self.budget = budget
        self.min_memory = min_memory
        self.min_cpu = min_cpu
        self.min_gpu = min_gpu
        self.context = context
        self.key_path = key_path

        self.file_path = path if path is not None and os.path.isfile(path) else None
        self.dir_path = path if path is not None and os.path.isdir(path) else None
        self.has_setup = self.dir_path is not None and os.path.exists(os.path.join(self.dir_path, 'setup.py'))

        if not _from_config:
            self._validate()
            if S3Client().bucket_exists(self.arn):
                raise ValueError('Project already exists. Destroy the project first or choose another name.')

        # Project details
        self.image_id = None
        self.platform_details = None
        self.instance_type = None
        self.instance_availability_zone = None
        self.instance_vcpu = None
        self.instance_memory = None
        self.instance_price = None

        # Key pair
        if self.key_path is None:
            self.key_name = 'key-pair-{}'.format(self.arn)
        else:
            self.key_name = os.path.basename(self.key_path)[:-4]
        self.key_id = None
        self.key_material = None

        # Security group
        self.group_name = 'group-{}'.format(self.arn)
        self.group_id = None

        # IAM role and instance profile
        self.role_name = 'role-{}'.format(self.arn)
        self.role_id = None
        self.role_arn = None
        self.instance_profile_name = 'profile-{}'.format(self.arn)
        self.instance_profile_arn = None

        # Boto clients
        self.ec2: Ec2Client = None
        self.bucket: S3Bucket = None
        self.iam: IamClient = None

        self.initialized = False
        self.start_time = None
        self.terminated = False

        self._init_aws_clients()

        if not _from_config:
            self._save_config()

    @staticmethod
    def _format_name(name):
        name = ''.join([c if c.isalnum() else '-' for c in name.lower()])
        name = name.strip('-')
        return name

    @staticmethod
    def _get_arn(name):
        return '{}-{}'.format(KEY, name)

    def _validate(self):
        if self.src is None and self.path is None:
            raise ValueError('You need to provide either src or path')
        elif self.src is not None and self.path is not None:
            raise ValueError('You can either provide one of src and path')
        elif self.src is not None:
            if self.entry_point is not None:
                raise ValueError('entry_point not accepted when providing src')
        elif self.path is not None:
            if not os.path.exists(self.path):
                raise FileNotFoundError('path does not exists')

            if self.dir_path is not None:
                if self.entry_point is None:
                    raise ValueError('entry_point needed when providing a directory path')
            else:
                if self.entry_point is not None:
                    raise ValueError('entry_point not accepted when providing a file path')

        if self.n_instances is not None and self.duration is not None and self.budget is not None:
            raise ValueError('You need to provide only two of n_instances, duration and budget')

        if self.has_setup and self.dependencies is not None:
            raise ValueError('You should not provide dependencies when your path has a setup.py')

    def _save_config(self):
        config = dict(
            name=self.functional_name,
            src=self.src,
            path=self.path,
            entry_point=self.entry_point,
            dependencies=self.dependencies,
            env_vars=self.env_vars,
            python=self.python,
            n_instances=self.n_instances,
            key_path=self.key_path,
            duration=self.duration,
            budget=self.budget,
            min_memory=self.min_memory,
            min_cpu=self.min_cpu,
            min_gpu=self.min_gpu,
            context=self.context.data if self.context is not None else None
        )
        status = dict(
            name=self.name,
            status=self.get_status(),
            start_time=self.start_time
        )
        self.bucket.save(config, 'doppel.config')
        self.bucket.save(status, 'doppel.status')
        if self.key_material is not None:
            self.bucket.save(self.key_material, 'key.pem')

    def get_status(self):
        status = 'initialized'
        if self.terminated:
            status = 'terminated'
        elif self.start_time is not None:
            status = 'running'
        return status

    @classmethod
    def exists(cls, name):
        name = cls._format_name(name)
        arn = cls._get_arn(name)
        return S3Client().bucket_exists(arn)

    @classmethod
    def load(cls, name):
        name = cls._format_name(name)
        arn = cls._get_arn(name)
        if not S3Client().bucket_exists(arn):
            raise ValueError('Project {} does not exists.'.format(name))

        bucket = S3Bucket(arn)
        config = bucket.load_json('doppel.config')
        config['context'] = DoppelContext(config['context'])
        project = cls(_from_config=True, **config)

        status = bucket.load_json('doppel.status')
        if status['start_time'] is not None:
            project.start_time = datetime.fromisoformat(status['start_time'])
        project.terminated = (status['status'] == 'termminated')

        if bucket.exists('key.pem'):
            project.key_material = bucket.load('key.pem')
        return project

    def _init_aws_clients(self):
        self.ec2 = Ec2Client()
        self.bucket = S3Bucket(self.arn)
        self.bucket.block_public_access()
        self.iam = IamClient()

    def init(self):
        self._init_image()
        self._init_instance()
        self._init_project()
        self.initialized = True

    def _init_image(self):
        image = self.ec2.get_latest_deep_learning_image()
        self.image_id = image[Ec2.IMAGE_ID]
        self.platform_details = image[Ec2.PLATFORM_DETAILS]

    def _init_instance(self):
        instances = self.ec2.get_instance_types()
        instances = instances[instances[Ec2.SUPPORTED_USAGES].apply(lambda x: 'spot' in x)]
        instances[Ec2.MEMORY_INFO] = np.round(instances[Ec2.MEMORY_INFO] / 1024)

        if self.min_memory is not None:
            instances = instances[instances[Ec2.MEMORY_INFO] >= self.min_memory]
        if self.min_cpu is not None:
            instances = instances[instances[Ec2.VCPU_INFO] >= self.min_cpu]
        if self.min_gpu is not None:
            def valid_gpu_instance(gpu):
                if pd.isnull(gpu):
                    return False
                return gpu[0]['Count'] >= self.min_gpu
            instances = instances[instances[Ec2.GPU_INFO].apply(valid_gpu_instance)]

        prices = self.ec2.get_spot_prices(products_description=self.platform_details)
        instances = pd.merge(instances, prices, on=Ec2.INSTANCE_TYPE)
        instances = instances.sort_values(Ec2.SPOT_PRICE)
        instance = instances.iloc[[0]].to_dict(orient='records')[0]

        self.instance_type = instance[Ec2.INSTANCE_TYPE]
        self.instance_availability_zone = instance[Ec2.AVAILABILITY_ZONE]
        self.instance_vcpu = instance[Ec2.VCPU_INFO]
        self.instance_memory = instance[Ec2.MEMORY_INFO]
        self.instance_price = instance[Ec2.SPOT_PRICE]

        logger.info('Selecting {} instance in {} [{:.0f} CPUs, {:.1f}Go, {:.4f}€/h]'.format(
            self.instance_type,
            self.instance_availability_zone,
            self.instance_vcpu,
            self.instance_memory,
            self.instance_price))

    def _init_project(self):
        n_none = (self.n_instances is None) + (self.duration is None) + (self.budget is None)
        if n_none == 1:
            if self.n_instances is None:
                self._compute_n_instances()
            elif self.duration is None:
                self._compute_duration()
            elif self.budget is None:
                self._compute_budget()
        elif n_none == 2:
            if self.n_instances is not None:
                pass
            elif self.duration is not None:
                self.n_instances = 1
                self._compute_budget()
            elif self.budget is not None:
                self.n_instances = 1
                self._compute_duration()
        elif n_none == 3:
            self.n_instances = 1

        if self.duration is None:
            logger.info('Running {} instances indefinitly, for {}€/hour'.format(
                self.n_instances, self.n_instances * self.instance_price))
        else:
            logger.info('Running {} instances for {:.1f} hours, for {:.2f}€'.format(
                self.n_instances, self.duration, self.budget))

    def _compute_n_instances(self):
        self.n_instances = (int)(np.round(self.budget / (self.duration * self.instance_price), 0))
        self._compute_duration()

    def _compute_duration(self):
        self.duration = self.budget / (self.n_instances * self.instance_price)

    def _compute_budget(self):
        self.budget = self.n_instances * self.duration * self.instance_price

    def start(self):
        if not self.initialized:
            self.init()

        self.terminated = False
        self.start_time = datetime.now()

        self._create_aws_resources()
        self._push_code_to_s3()
        self._save_config()

        instance_dns = self._start_instances(self.n_instances)
        self._configure_instances(instance_dns)

    def _create_aws_resources(self):
        if self.key_path is None:
            self._create_key_pair()
        else:
            self._load_key_pair()
        self._create_security_group()
        self._create_role()
        self._create_instance_profile()

    def _load_key_pair(self):
        with open(self.key_path) as stream:
            self.key_material = stream.read()

    def _create_key_pair(self):
        key = self.ec2.create_key_pair(self.key_name, tag=(KEY, self.name))
        self.key_id = key['KeyPairId']
        self.key_material = key['KeyMaterial']

    def _create_security_group(self):
        group = self.ec2.create_security_group(
            self.group_name, 'Security for {} {}'.format(KEY, self.functional_name),
            tag=(KEY, self.name)
        )
        self.group_id = group['GroupId']
        self.ec2.add_ssh_access_to_my_ip_to_security_group(self.group_id)

    def _create_role(self):
        role = self.iam.create_role(self.role_name, service='ec2',
                                    description='Role for {} {}'.format(KEY, self.functional_name),
                                    tag=(KEY, self.name))
        self.role_id = role['RoleId']
        self.role_arn = role['Arn']
        self.iam.attach_role_policy(self.role_name, Policy.S3)
        self.iam.attach_role_policy(self.role_name, Policy.CLOUD_WATCH)

    def _create_instance_profile(self):
        profile = self.iam.create_instance_profile(self.instance_profile_name, self.role_name)
        self.instance_profile_arn = profile['Arn']

    def _push_code_to_s3(self):
        if self.src is not None:
            self.bucket.save(self.src, 'main.py')
        elif self.file_path is not None:
            self.bucket.upload(self.file_path, 'main.py')
        elif self.dir_path is not None:
            zip = zip_dir(self.dir_path)
            self.bucket.save(zip, 'src.zip')

        if self.dependencies is not None:
            self.bucket.save('\n'.join(self.dependencies), 'requirements.txt')

        with open(os.path.join(get_root_path(), 'awslogs/awscli.conf')) as file:
            aws_cli = file.read()
        aws_cli = aws_cli.format(region=self.ec2.region)

        with open(os.path.join(get_root_path(), 'awslogs/awslogs.conf')) as file:
            aws_logs = file.read()
        log_group = '{}-{}'.format(KEY, self.name)
        log_group_name = log_group
        aws_logs = aws_logs.format(log_group=log_group, log_group_name=log_group_name)

        self.bucket.save(aws_cli, 'awscli.conf')
        self.bucket.save(aws_logs, 'awslogs.conf')

    def _start_instances(self, n):
        instances = self.ec2.run_spot_instances(
            ami_id=self.image_id, instance_type=self.instance_type, availability_zone=self.instance_availability_zone,
            key_name=self.key_name, group_name=self.group_name, instance_profile_arn=self.instance_profile_arn,
            n_instances=n, tag=(KEY, self.name)
        )
        instance_dns = [instance[Ec2.PUBLIC_DNS] for instance in instances]
        return instance_dns

    def _configure_instances(self, instance_dns):
        if len(instance_dns) == 1:
            self._configure_instance(instance_dns[0])
        else:
            threads = [threading.Thread(target=self._configure_instance, args=(dns,)) for dns in instance_dns]
            [thread.start() for thread in threads]
            [thread.join() for thread in threads]

    def _configure_instance(self, dns):
        logger.info('Configuring instance {}'.format(dns))
        ssh = SshSession(dns, Ec2.USER, key=self.key_material)
        ssh.connect()

        # Init
        ssh.mkdir(KEY)
        with ssh.cd(KEY):
            ssh.mkdir('data')
            ssh.mkdir('src')
            ssh.run("echo \"Instance started\" > logs")

        if self.context is not None:
            with ssh.cd(KEY, 'data'):
                for key, data in self.context.data.items():
                    ssh.run('aws s3 cp s3://{}/{} .'.format(data['bucket'], key))

        # Update
        # ssh.run('sudo yum -y update')

        # Configure logging
        # ssh.run('sudo yum install -y awslogs')
        with ssh.cd('/etc/awslogs'):
            ssh.run('sudo aws s3 cp s3://{}/awscli.conf .'.format(self.arn))
            ssh.run('sudo aws s3 cp s3://{}/awslogs.conf .'.format(self.arn))
        ssh.run('sudo systemctl start awslogsd')

        # Retrieve source
        with ssh.cd(KEY):
            if self.src is not None or self.file_path is not None:
                ssh.run('aws s3 cp s3://{}/starter.py src/starter.py'.format(self.arn))
            elif self.dir_path is not None:
                ssh.run('aws s3 cp s3://{}/src.zip src.zip'.format(self.arn))
                ssh.run('unzip src.zip -d src')

        # Create virtual env
        python = '' if self.python is None else '={}'.format(self.python)
        ssh.run('yes | conda create -n {} python{}'.format(KEY, python))

        # Install dependencies
        with ssh.cd(KEY, 'src'), ssh.activate(KEY):
            if self.dependencies is not None:
                ssh.run('aws s3 cp s3://{}/requirements.txt .'.format(self.arn))
                ssh.run('pip install -r requirements.txt')
            elif self.has_setup:
                ssh.run('python setup.py install')

        # Run
        with ssh.activate(KEY), ssh.connection.prefix(self._export_env_vars()), ssh.cd(KEY, 'src'):
            ssh.python(self.entry_point or 'starter.py', disown=True)

    def _export_env_vars(self):
        if self.env_vars is None:
            env_vars = {}
        else:
            env_vars = self.env_vars.copy()
        env_vars['DOPPEL'] = 'true'
        env_vars['DOPPEL_NAME'] = self.arn
        return ' && '.join(['export {}={}'.format(k, v) for k, v in env_vars.items()])

    def status(self):
        instances = self.ec2.get_instances_by_tag(KEY, self.name)
        print('-------- {}: {} instances'.format(self.name, len(instances)))
        for instance in instances:
            launch = instance['LaunchTime']
            runtime = datetime.now(launch.tzinfo) - launch
            print('[{}] {}, launched {:.1f} hours ago'.format(instance['InstanceId'], instance['State']['Name'], runtime.total_seconds()/3600))

    def monitore(self):
        if not self.initialized:
            self.init()

        while True:
            if self.duration is not None and self._get_duration() > self.duration:
                logger.info('Terminating project...')
                self.terminate()
                break
            states = self.ec2.get_instances_by_tag(KEY, self.name, attribute=['State', 'Name'])
            n_running = len([state for state in states if state == 'running'])
            logger.info('{} instances running'.format(n_running))
            missing_instances = self.n_instances - n_running
            if missing_instances > 0:
                logger.info('Starting {} new instances'.format(missing_instances))
                instance_dns = self._start_instances(missing_instances)
                self._configure_instances(instance_dns)

            time.sleep(300)

    def _get_duration(self):
        if self.start_time is None:
            raise ValueError('project not started')
        return (datetime.now() - self.start_time).total_seconds() / 3600

    def terminate(self):
        instance_ids = self.ec2.get_instances_by_tag(KEY, self.name, 'InstanceId')
        if len(instance_ids) > 0:
            self.ec2.terminate_instances(instance_ids)
        self.ec2.delete_key_pair_by_tag(KEY, self.name)
        self.ec2.delete_security_group_by_tag(KEY, self.name)
        self.iam.delete_instance_profile(self.instance_profile_name)
        self.iam.delete_role(self.role_name)
        self.terminated = True
        self._save_config()

    def destroy(self):
        self.terminate()
        self.bucket.empty()
        self.bucket.delete()
