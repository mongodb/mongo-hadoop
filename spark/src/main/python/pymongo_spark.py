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

__version__ = '0.1'

import sys

import py4j
import pyspark


# These types need special pickling to work correctly with PyMongo.
_PICKLE_BSON_TYPES = (
    'org.bson.types.ObjectId',
    'org.bson.types.Binary',
    'org.bson.types.Code',
    'org.bson.types.CodeWScope',
    'org.bson.types.CodeWithScope',
    'org.bson.types.MaxKey',
    'org.bson.types.MinKey',
    'org.bson.types.BSONTimestamp',
    'com.mongodb.DBRef',
    'java.util.regex.Pattern'
)


# Register Constructors for unpickling.
# (module, class)
_UNPICKLE_CONSTRUCTORS = (
    ('bson.binary', 'Binary'),
    ('bson.code', 'Code'),
    ('bson.dbref', 'DBRef'),
    ('bson.int64', 'Int64'),
    ('bson.max_key', 'MaxKey'),
    ('bson.min_key', 'MinKey'),
    ('bson.timestamp', 'Timestamp'),
    ('bson.regex', 'Regex'),
    ('bson.objectid', 'ObjectId')
)


def _ensure_pickles(self):
    if not getattr(self, '__registered_picklers', False):
        try:
            jvm = self._jvm
            pickler = jvm.net.razorvine.pickle.Pickler
            bson_pickler = jvm.com.mongodb.spark.pickle.BSONPickler()

            for pbt in _PICKLE_BSON_TYPES:
                pickler.registerCustomPickler(
                    jvm.java.lang.Class.forName(pbt), bson_pickler)

            unpickler = jvm.net.razorvine.pickle.Unpickler
            for unpc in _UNPICKLE_CONSTRUCTORS:
                unpickler.registerConstructor(
                    unpc[0], unpc[1],
                    jvm.java.lang.Class.forName(
                        'com.mongodb.spark.pickle.%sConstructor' % unpc[1])
                    .newInstance())
            self.__registered_picklers = True
        except py4j.protocol.Py4JError:
            orig_t, orig_v, orig_tb = sys.exc_info()
            try:
                # Try to guess most common cause of failure.
                raise (py4j.protocol.Py4JError,
                       "Error while communicating with the JVM. "
                       "Is the MongoDB Spark jar on Spark's CLASSPATH? : " +
                       str(orig_v),
                       orig_tb)
            finally:
                # Avoid circular reference with traceback.
                del orig_tb


def saveToMongoDB(self, connection_string, config=None):
    """Save this RDD to MongoDB."""
    conf = {'mongo.output.uri': connection_string}
    if config:
        conf.update(config)
    # Hadoop RDD elements MUST be pairs (i.e. 2-tuples).
    sample = self.first()
    to_save = self
    keyClass = 'com.mongodb.hadoop.io.BSONWritable'
    if not (isinstance(sample, tuple) and len(sample) == 2):
        # The MongoDB Hadoop Connector will ignore null keys.
        to_save = self.map(lambda x: (None, x))
        keyClass = 'org.apache.hadoop.io.NullWritable'
    to_save.saveAsNewAPIHadoopFile(
        'file:///this-is-unused',
        outputFormatClass='com.mongodb.hadoop.MongoOutputFormat',
        keyClass=keyClass,
        valueClass='com.mongodb.hadoop.io.BSONWritable',
        keyConverter='com.mongodb.spark.pickle.NoopConverter',
        valueConverter='com.mongodb.spark.pickle.NoopConverter',
        conf=conf)


def saveToBSON(self, file_path, config=None):
    """Save this RDD as a BSON file."""
    # Hadoop RDD elements MUST be pairs (i.e. 2-tuples).
    sample = self.first()
    to_save = self
    keyClass = 'com.mongodb.hadoop.io.BSONWritable'
    if not (isinstance(sample, tuple) and len(sample) == 2):
        # The MongoDB Hadoop Connector will ignore null keys.
        to_save = self.map(lambda x: (None, x))
        keyClass = 'org.apache.hadoop.io.NullWritable'
    to_save.saveAsNewAPIHadoopFile(
        file_path,
        outputFormatClass='com.mongodb.hadoop.BSONFileOutputFormat',
        keyClass=keyClass,
        valueClass='com.mongodb.hadoop.io.BSONWritable',
        keyConverter='com.mongodb.spark.pickle.NoopConverter',
        valueConverter='com.mongodb.spark.pickle.NoopConverter',
        conf=config
    )


def BSONFilePairRDD(self, file_path, config=None):
    """Create a pair RDD backed by a BSON file."""
    _ensure_pickles(self)
    return self.newAPIHadoopFile(
        file_path,
        inputFormatClass='com.mongodb.hadoop.BSONFileInputFormat',
        keyClass='com.mongodb.hadoop.io.BSONWritable',
        valueClass='com.mongodb.hadoop.io.BSONWritable',
        conf=config)


def mongoPairRDD(self, connection_string, config=None):
    """Create a pair RDD backed by MongoDB."""
    _ensure_pickles(self)
    conf = {'mongo.input.uri': connection_string}
    if config:
        conf.update(config)
    return self.newAPIHadoopRDD(
        inputFormatClass='com.mongodb.hadoop.MongoInputFormat',
        keyClass='com.mongodb.hadoop.io.BSONWritable',
        valueClass='com.mongodb.hadoop.io.BSONWritable',
        conf=conf)


def BSONFileRDD(self, file_path, config=None):
    """Create an RDD backed by a BSON file."""
    return self.BSONFilePairRDD(file_path, config).values()


def mongoRDD(self, connection_string, config=None):
    """Create an RDD backed by MongoDB."""
    return self.mongoPairRDD(connection_string, config).values()


def activate():
    """Activate integration between PyMongo and PySpark.
    This function only needs to be called once.
    """
    # Patch methods in rather than extending these classes.  Many RDD methods
    # result in the creation of a new RDD, whose exact type is beyond our
    # control. However, we would still like to be able to call any of our
    # methods on the resulting RDDs.
    pyspark.rdd.RDD.saveToMongoDB = saveToMongoDB
    pyspark.rdd.RDD.saveToBSON = saveToBSON
    pyspark.context.SparkContext.BSONFileRDD = BSONFileRDD
    pyspark.context.SparkContext.BSONFilePairRDD = BSONFilePairRDD
    pyspark.context.SparkContext.mongoRDD = mongoRDD
    pyspark.context.SparkContext.mongoPairRDD = mongoPairRDD
