import json
import boto3
import pandas as pd
from pkg_resources import resource_filename

from doppel.aws.__init__ import AwsClient


class PricingClient(AwsClient):

    def __init__(self, profile_name=None):
        super().__init__('pricing', profile_name)
        self.client = boto3.client('pricing', region_name='us-east-1')

    def get_linux_prices(self, instanceType):
        data = self.client.get_products(
            ServiceCode='AmazonEC2',
            Filters=[
                {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': self._get_region_name()},
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instanceType},
            ]
        )
        data = [self._extract_price(p) for p in data['PriceList']]
        return pd.json_normalize(data)

    def _get_region_name(self):
        endpoint_file = resource_filename('botocore', 'data/endpoints.json')
        with open(endpoint_file, 'r') as f:
            data = json.load(f)
        region_name = data['partitions'][0]['regions'][self.region]['description']
        region_name = region_name.replace('Europe', 'EU')
        return region_name

    def _extract_price(self, price):
        price = json.loads(price)
        ondemand = price['terms']['OnDemand']
        code0 = list(ondemand)[0]
        code1 = list(ondemand[code0]['priceDimensions'])[0]
        return {
            'product': price['product'],
            'price': price['terms']['OnDemand'][code0]['priceDimensions'][code1]
        }
