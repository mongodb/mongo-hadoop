from setuptools import setup

setup(
    name='PyMongo Hadoop Support',
    version='1.0.0',
    maintainer='Brendan W. McAdams',
    maintainer_email='brendan@10gen.com',
    packages=['pymongo_hadoop'],
    url='https://github.com/mongodb/mongo-hadoop',
    requires=[
        'pymongo'
    ],
)
