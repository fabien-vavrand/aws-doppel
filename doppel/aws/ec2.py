import time
from retry import retry
import pandas as pd
from datetime import datetime, timedelta

from doppel.aws import AwsClient
from doppel.aws.utils import get_list_param, get_next_token, get_my_ip, get_filters, get_tag


class Ec2:

    USER = 'ec2-user'
    IMAGE_ID = 'ImageId'
    PLATFORM_DETAILS = 'PlatformDetails'
    SUPPORTED_USAGES = 'SupportedUsageClasses'
    INSTANCE_TYPE = 'InstanceType'
    SPOT_PRICE = 'SpotPrice'
    AVAILABILITY_ZONE = 'AvailabilityZone'
    MEMORY_INFO = 'MemoryInfo.SizeInMiB'
    VCPU_INFO = 'VCpuInfo.DefaultVCpus'
    GPU_INFO = 'GpuInfo.Gpus'
    PUBLIC_DNS = 'PublicDnsName'


class Ec2Client(AwsClient):

    def __init__(self):
        super().__init__('ec2')

    def get_regions(self):
        return self.client.describe_regions()['Regions']

    def get_instance_types(self):
        response = self.client.describe_instance_types()
        instances = response['InstanceTypes']
        while get_next_token(response) != '':
            response = self.client.describe_instance_types(NextToken=response['NextToken'])
            instances.extend(response['InstanceTypes'])
        instances = pd.json_normalize(instances)
        return instances

    def get_image(self, ami_id):
        images = self.get_images([ami_id])
        return images[0]

    def get_images(self, ami_ids):
        images = self.client.describe_images(
            ImageIds=ami_ids
        )
        return images['Images']

    def get_latest_linux_image(self):
        return self._get_latest_image(pattern='amzn2-ami-hvm-*')

    def get_latest_deep_learning_image(self):
        return self._get_latest_image(pattern='Deep Learning AMI (Amazon Linux 2) *')

    def _get_latest_image(self, pattern):
        filters = {
            'name': pattern,
            'architecture': 'x86_64',
            'state': 'available',
            'root-device-type': 'ebs',
            'virtualization-type': 'hvm',
            'hypervisor': 'xen',
            'image-type': 'machine',
            'block-device-mapping.volume-type': 'gp2'
        }

        images = self.client.describe_images(Owners=['amazon'], Filters=get_filters(filters))
        images = sorted(images['Images'], key=lambda i: i['CreationDate'])
        return images[-1]

    def get_spot_prices(self, instance_types=None, products_description=None, days=None):
        prices = []
        response = None
        start_time = datetime.today()
        if days is not None:
            start_time = start_time - timedelta(days=days)
        while response is None or response['NextToken'] != '':
            response = self.client.describe_spot_price_history(
                InstanceTypes=get_list_param(instance_types),
                ProductDescriptions=get_list_param(products_description),
                StartTime=start_time,
                NextToken=get_next_token(response)
            )
            prices.extend(response['SpotPriceHistory'])
        prices = pd.json_normalize(prices)
        prices['SpotPrice'] = pd.to_numeric(prices['SpotPrice'])
        prices = prices.sort_values(['InstanceType', 'AvailabilityZone', 'ProductDescription', 'Timestamp'])
        return prices

    def run_spot_instances(self, ami_id, instance_type, availability_zone, key_name, group_name, instance_profile_arn,
                           n_instances=1, tag=None):
        instances = self.client.run_instances(
            InstanceType=instance_type,
            ImageId=ami_id,
            KeyName=key_name,
            MinCount=n_instances,
            MaxCount=n_instances,
            InstanceMarketOptions={
                'MarketType': 'spot'
            },
            Placement={
                'AvailabilityZone': availability_zone
            },
            SecurityGroups=[group_name],
            TagSpecifications=get_tag(tag, 'instance')
        )
        instances = instances['Instances']
        instance_ids = [instance['InstanceId'] for instance in instances]

        while any([status != 'running' for status in self.get_instances(instance_ids, ['State', 'Name'])]):
            time.sleep(1)

        for instance_id in instance_ids:
            self.associate_iam_instance_profile(instance_id, instance_profile_arn)

        instances = self.get_instances(instance_ids)
        return instances

    @retry(delay=1)
    def associate_iam_instance_profile(self, instance_id, profile_arn):
        self.client.associate_iam_instance_profile(
            InstanceId=instance_id,
            IamInstanceProfile={
                'Arn': profile_arn
            }
        )

    def get_instances(self, instance_ids, attribute=None):
        reservations = self.client.describe_instances(InstanceIds=instance_ids)
        return self._get_instances(reservations, attribute)

    def get_instances_by_tag(self, key, value, attribute=None):
        reservations = self.client.describe_instances(Filters=get_filters({'tag:' + key: value}))
        return self._get_instances(reservations, attribute)

    def _get_instances(self, reservations, attribute):
        reservations = reservations['Reservations']
        instances = []
        for reservation in reservations:
            instances.extend(reservation['Instances'])
        if attribute is not None:
            if isinstance(attribute, str):
                attribute = [attribute]
            for a in attribute:
                instances = [instance[a] for instance in instances]
        return instances

    def terminate_instances(self, instance_ids, wait_for_termination=True):
        self.client.terminate_instances(InstanceIds=instance_ids)
        if wait_for_termination:
            while any([status != 'terminated' for status in self.get_instances(instance_ids, ['State', 'Name'])]):
                time.sleep(1)

    def create_key_pair(self, name, tag=None):
        response = self.client.create_key_pair(
            KeyName=name,
            TagSpecifications=get_tag(tag, 'key-pair')
        )
        return response

    def delete_key_pair_by_name(self, name):
        self.client.delete_key_pair(KeyName=name)

    def delete_key_pair_by_tag(self, key, value):
        key_pairs = self.client.describe_key_pairs(Filters=get_filters({'tag:' + key: value}))
        for key_pair in key_pairs['KeyPairs']:
            self.delete_key_pair_by_name(key_pair['KeyName'])

    def create_security_group(self, name, description='', tag=None):
        response = self.client.create_security_group(
            GroupName=name,
            Description=description,
            TagSpecifications=get_tag(tag, 'security-group')
        )
        return response

    def add_ssh_access_to_my_ip_to_security_group(self, group_id):
        self.client.authorize_security_group_ingress(
            GroupId=group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 22,
                    'ToPort': 22,
                    'IpRanges': [
                        {
                            'CidrIp': '{}/32'.format(get_my_ip()),
                            'Description': 'SSH access'
                        }
                    ]
                }
            ])

    def delete_security_group_by_name(self, name):
        self.client.delete_security_group(GroupName=name)

    def delete_security_group_by_tag(self, key, value):
        groups = self.client.describe_security_groups(Filters=get_filters({'tag:' + key: value}))
        for group in groups['SecurityGroups']:
            self.delete_security_group_by_name(group['GroupName'])
