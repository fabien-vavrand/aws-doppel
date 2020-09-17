
import os
import datetime
import time
import pandas as pd

from doppel.core.context import DoppelContext

context = DoppelContext()
context.add_data(key='titanic.csv', bucket='doppel-project-data', source=r'C:\data\titanic\train.csv')

logger = context.get_logger()
logger.info('DOPPEL = {}'.format(os.getenv('DOPPEL')))
logger.info('DOPPEL_NAME = {}'.format(os.getenv('DOPPEL_NAME')))

data = pd.read_csv(context.data_path('titanic.csv'))
logger.info('Loaded data: {} rows'.format(len(data)))

while True:
    logger.info("Alive: {}".format(datetime.datetime.now()))
    time.sleep(60)
