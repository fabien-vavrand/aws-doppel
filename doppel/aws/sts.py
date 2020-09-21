from doppel.aws.__init__ import AwsClient


class StsClient(AwsClient):

    def __init__(self, profile_name=None):
        super().__init__('sts', profile_name)

    def get_account_id(self):
        account = self.client.get_caller_identity()
        return account['Account']
