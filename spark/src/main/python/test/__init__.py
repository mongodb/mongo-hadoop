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

import os
import sys

from glob import glob

if sys.version_info[:2] == (2, 6):
    # Support testing with Python 2.6.
    import unittest2 as unittest
else:
    import unittest


# pyspark ships with Spark and isn't installed like a normal Python package.
def find_pyspark():
    """Add pyspark and py4j to the PYTHONPATH."""
    spark_home = os.environ.get('SPARK_HOME')
    if spark_home is None:
        raise ImportError(
            'Cannot import pyspark because SPARK_HOME is not set.')
    spark_python = os.path.join(spark_home, 'python')
    py4j = glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
    sys.path[:0] = [spark_python, py4j]


def find_mh_spark_jar():
    """Add mongo-hadoop-spark-*.jar to Spark's CLASSPATH."""
    curdir = os.path.dirname(os.path.abspath(__file__))
    # Find directory that contains the "core" module in the project.
    while (not os.path.exists(os.path.join(curdir, 'core')) and
           os.path.exists(os.path.join(curdir, os.path.pardir))):
        curdir = os.path.join(curdir, os.path.pardir)
    if not os.path.exists(os.path.join(curdir, 'core')):
        raise OSError(
            'Could not find "core" module in mongo-hadoop project.')
    # "spark" module is in the same directory. Add jars from that module.
    jars = glob(
        os.path.join(curdir, 'spark', 'build', 'libs', '*.jar'))
    jars_str = ','.join(jars)
    # Spark < 1.4 needs both flags. See SPARK-4924.
    pyspark_submit_args = (
        '--jars %s --driver-class-path %s pyspark-shell'
        % (jars_str, jars_str))
    os.environ['PYSPARK_SUBMIT_ARGS'] = pyspark_submit_args

# Make pyspark importable
find_pyspark()
import pyspark

# Add appropriate jars to Spark's CLASSPATH.
find_mh_spark_jar()

MONGO_HOST = os.environ.get("DB_IP", "localhost")
MONGO_PORT = int(os.environ.get("DB_PORT", "27017"))
CONNECTION_STRING = ('mongodb://%s:%d/mongo_hadoop.test'
                     % (MONGO_HOST, MONGO_PORT))
