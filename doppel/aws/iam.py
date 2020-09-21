import json
from doppel.aws.__init__ import AwsClient
from doppel.aws.utils import get_tag
from doppel.aws.sts import StsClient


class Policy:

    ADMINISTRATOR = 'AdministratorAccess'
    EC2 = 'AmazonEC2FullAccess'
    S3 = 'AmazonS3FullAccess'
    CLOUD_WATCH = 'CloudWatchFullAccess'
    CODE_DEPLOY = 'AWSCodeDeployFullAccess'


class IamClient(AwsClient):

    def __init__(self, profile_name=None):
        super().__init__('iam', profile_name)

    def get_user(self):
        user = self.client.get_user()['User']
        return user

    def get_user_id(self):
        user = self.get_user()
        user_id = user['Arn'].split(':')[4]
        return user_id

    def create_role(self, role_name, service, description='', tag=None):
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "{}.amazonaws.com".format(service)
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        role = self.client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(policy),
            Description=description,
            Tags=get_tag(tag)
        )
        return role['Role']

    def attach_role_policy(self, role_name, policy_name):
        response = self.client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=self.get_policy_arn(policy_name)
        )
        return response

    def get_policy_arn(self, policy_name):
        return self._get_policy_arn('aws', policy_name)

    def get_account_policy_arn(self, policy_name):
        account_id = StsClient().get_account_id()
        return self._get_policy_arn(account_id, policy_name)

    def _get_policy_arn(self, account, name):
        return 'arn:aws:iam::{}:policy/{}'.format(account, name)

    def get_role(self, role_name):
        try:
            role = self.client.get_role(RoleName=role_name)
            return role
        except self.client.exceptions.NoSuchEntityException:
            return None

    def delete_role(self, role_name):
        if self.get_role(role_name) is None:
            return None

        policies = self.client.list_attached_role_policies(RoleName=role_name)
        for policy in policies['AttachedPolicies']:
            self.client.detach_role_policy(RoleName=role_name, PolicyArn=policy['PolicyArn'])

        response = self.client.delete_role(RoleName=role_name)
        return response

    def create_instance_profile(self, name, role_name):
        self.client.create_instance_profile(InstanceProfileName=name)
        self.client.add_role_to_instance_profile(
            InstanceProfileName=name,
            RoleName=role_name
        )
        profile = self.get_instance_profile(name)
        return profile

    def get_instance_profile(self, name):
        try:
            profile = self.client.get_instance_profile(InstanceProfileName=name)
            return profile['InstanceProfile']
        except self.client.exceptions.NoSuchEntityException:
            return None

    def get_instance_profile_for_role(self, role_name):
        profiles = self.client.list_instance_profiles_for_role(RoleName=role_name)
        return profiles['InstanceProfiles']

    def delete_instance_profile(self, name):
        profile = self.get_instance_profile(name)
        if profile is None:
            return None

        for role in profile['Roles']:
            self.client.remove_role_from_instance_profile(InstanceProfileName=name, RoleName=role['RoleName'])
        response = self.client.delete_instance_profile(InstanceProfileName=name)
        return response
