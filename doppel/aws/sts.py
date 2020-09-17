from doppel.aws import AwsClient


class StsClient(AwsClient):

    def __init__(self):
        super().__init__('sts')

    def get_account_id(self):
        account = self.client.get_caller_identity()
        return account['Account']
