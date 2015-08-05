#!/usr/bin/python
# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

_classifiers = """
Development Status :: 4 - Beta
Intended Audience :: Developers
License :: OSI Approved :: Apache Software License
Operating System :: OS Independent
Programming Language :: Python :: 2.6
Programming Language :: Python :: 2.7
Topic :: Database :: Front-Ends
Topic :: Scientfic/Engineering :: Interface Engine/Protocol Translator
"""

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

extra_opts = {}
try:
    with open('README.rst', 'r') as fd:
        extra_opts['long_description'] = fd.read()
except IOError:
    pass        # Install without README.rst

setup(
    name='pymongo-spark',
    version='0.1.dev0',
    author='MongoDB, Inc.',
    author_email='mongodb-user@googlegroups.com',
    description='Utilities for using Spark with PyMongo',
    keywords=['spark', 'mongodb', 'mongo', 'hadoop', 'pymongo'],
    license="http://www.apache.org/licenses/LICENSE-2.0.html",
    platforms=['any'],
    url='https://github.com/mongodb/mongo-hadoop',
    install_requires=['pymongo>=3.0.3'],
    packages=find_packages(exclude=('test',)),
    classifiers=_classifiers.splitlines(),
    test_suite='test',
    **extra_opts
)
