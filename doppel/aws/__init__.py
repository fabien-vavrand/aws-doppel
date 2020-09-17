import logging
import boto3


class AwsClient:

    def __init__(self, service, profile_name=None):
        self.service = service
        self.profile_name = profile_name
        self.session = boto3.session.Session(profile_name=self.profile_name)
        self.region = self.session.region_name
        self.client = boto3.client(self.service, region_name=self.region)
        try:
            self.resource = boto3.resource(self.service, region_name=self.region)
        except Exception as ex:
            self.resource = None
        self.logger = logging.getLogger('aws')

    def get_credentials(self):
        credentials = self.session.get_credentials()
        credentials = credentials.get_frozen_credentials()
        return credentials.access_key, credentials.secret_key
