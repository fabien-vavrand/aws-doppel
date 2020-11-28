# AWS Doppel
_Distribute and run code on AWS._

Deploying code to production is becoming easier, but leveraging cloud resources to train a large model or run dozen of instances in parallel to tune hyperparameters can still be a struggle, from choosing your instance type, to debugging and retrieving your logs. AWS Doppel makes it as easy and transparent as possible!

## One click deployment
AWS Doppel automatically creates all the AWS resources, provisions EC2 instances and run your code
* Cheapest spot or on-demand instances meeting your requirements (CPU, GPU, memory) are automatically selected
* Your code is automatically sent to your EC2 instances and installed via setup.py or added to the PYTHONPATH
* Context data are uploaded from your machine to a new S3 bucket, and downloaded to your EC2 instances
* Add requirements, installed via pip, or packages available on your local machine

```python
context = DoppelContext() \
    .add_data(key='train.pkl', bucket='doppel-project', source=r'C:\data\train.pkl') \
    .add_data(key='test.pkl', bucket='doppel-project', source=r'C:\data\test.pkl')
context.upload_data()

project = DoppelProject(
    name='doppel-project',
    path=r'..\my-project',
    entry_point='-m project.train',
    packages=[r'C:\app\aws-doppel'],
    python='3.7.6',
    n_instances=1,
    min_memory=128,
    context=context,
    env_vars={'PYTHONHASHSEED': '1'})
project.start()
```

## Transparent execution
Execute the same code locally or remotely using the doppel context. Depending on the environment:
* ```context.get_logger()``` redirects your logs to the console or to a new AWS Cloudwatch journal
* ```context.path()``` points to your local data source or to the instance data directory
* ```context.save()```, ```context.save_pickle()``` and ```context.save_json()``` saves your data to your local folder or your S3 bucket
* ```terminate(context)``` stops the EC2 instance and saves money!

```python
import logging
from doppel import terminate
from project.data import Loader


context.get_logger()
try:
    loader = Loader(context.path())
    X, y = loader.load()
    model = MyModel().fit(X, y)
    context.save(model.save(), 'my_model.pkl')

except Exception as e:
    logging.info(str(e))

finally:
    logging.info('Finished')
    terminate(context)
```