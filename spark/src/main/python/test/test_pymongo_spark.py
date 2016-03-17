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

import datetime
import os
import re
import shutil

import bson
import pymongo

import pymongo_spark
from test import pyspark, unittest, MONGO_HOST, MONGO_PORT, CONNECTION_STRING


class TestPyMongoSpark(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pymongo_spark.activate()
        cls.sc = pyspark.SparkContext(appName='test_pickle', master='local')
        cls.client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        cls.coll = cls.client.mongo_hadoop.test
        cls.output_coll = cls.client.mongo_hadoop.test.output

    def drop_collections(self):
        self.coll.drop()
        self.output_coll.drop()

    def setUp(self):
        self.drop_collections()

    def tearDown(self):
        self.drop_collections()

    def test_read_from_mongo(self):
        # Insert a document that contains one of each BSON type.
        tzinfo = bson.tz_util.FixedOffset(120, 'test-offset')
        self.coll.insert_one({
            '_id': bson.objectid.ObjectId(),
            'datetime': datetime.datetime.now(tzinfo),
            'binary': bson.binary.Binary('i am a binary', 42),
            'dbref': bson.dbref.DBRef('collectionname', 12),
            'maxkey': bson.max_key.MaxKey(),
            'minkey': bson.min_key.MinKey(),
            # No scope
            'code': bson.code.Code('function() { return true; }'),
            # With scope
            'code2': bson.code.Code('function() { return MyBool; }',
                                    {'MyBool': 'true'}),
            'ts': bson.timestamp.Timestamp(12345, 42),
            'regex': re.compile(r'[a-z]+', re.IGNORECASE | re.UNICODE)
        })
        actual = self.sc.mongoRDD(CONNECTION_STRING).first()
        self.assertEqual(self.coll.find_one(), actual)
        self.assertIsInstance(actual['_id'], bson.objectid.ObjectId)
        self.assertIsInstance(actual['datetime'], datetime.datetime)
        self.assertIsInstance(actual['binary'], bson.binary.Binary)
        self.assertIsInstance(actual['dbref'], bson.dbref.DBRef)
        self.assertIsInstance(actual['maxkey'], bson.max_key.MaxKey)
        self.assertIsInstance(actual['minkey'], bson.min_key.MinKey)
        self.assertIsInstance(actual['code'], bson.code.Code)
        self.assertIsInstance(actual['code2'], bson.code.Code)
        self.assertIsInstance(actual['ts'], bson.timestamp.Timestamp)
        self.assertIsInstance(actual['regex'], bson.regex.Regex)
        # Briefly test that the pair RDD works as well.
        self.assertEqual(actual,
                         self.sc.mongoPairRDD(CONNECTION_STRING).first()[1])

    def test_save_to_mongo(self):
        tzinfo = bson.tz_util.FixedOffset(160, 'test-offset')
        self.coll.insert_one({
            '_id': bson.objectid.ObjectId('55c8d60a6e32aba68881faae'),
            'datetime': datetime.datetime.now(tzinfo),
            'binary': bson.binary.Binary('i am a binary', 42),
            'dbref': bson.dbref.DBRef('collectionname', 12),
            'maxkey': bson.max_key.MaxKey(),
            'minkey': bson.min_key.MinKey(),
            # No scope
            'code': bson.code.Code('function() { return true; }'),
            # With scope
            'code2': bson.code.Code('function() { return MyBool; }',
                                    {'MyBool': 'true'}),
            'ts': bson.timestamp.Timestamp(12345, 42),
            'regex': re.compile(r'[a-z]+', re.IGNORECASE | re.UNICODE),
            'int64': bson.int64.Int64(2 ** 32)
        })

        # Try to save it to another collection.
        self.sc.mongoRDD(CONNECTION_STRING).saveToMongoDB(
            'mongodb://%s:%d/mongo_hadoop.test.output'
            % (MONGO_HOST, MONGO_PORT))
        self.assertEqual(self.coll.find_one(), self.output_coll.find_one())
        self.output_coll.drop()
        # Try saving from a pair RDD.
        self.sc.mongoPairRDD(CONNECTION_STRING).saveToMongoDB(
            'mongodb://%s:%d/mongo_hadoop.test.output'
            % (MONGO_HOST, MONGO_PORT))
        self.assertEqual(self.coll.find_one(), self.output_coll.find_one())

    def test_read_write_bson(self):
        self.coll.insert_many([{'_id': bson.objectid.ObjectId()}
                               for i in range(1000)])
        bson_location = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'spark_output')
        self.sc.mongoRDD(CONNECTION_STRING).saveToBSON(bson_location)
        try:
            # 'part-r-00000.bson' is a file name generated by Spark.
            bson_file = os.path.join(bson_location, 'part-r-00000.bson')
            with open(bson_file, 'rb') as fd:
                documents = bson.decode_all(fd.read())
                self.assertEqual(1000, len(documents))

            # Try loading the BSON file into Spark as a separate RDD.
            bson_rdd = self.sc.BSONFileRDD(bson_file)
            self.assertEqual(1000, bson_rdd.count())

            # Also try the pair version.
            bson_pair_rdd = self.sc.BSONFilePairRDD(bson_file)
            self.assertEqual(1000, bson_pair_rdd.count())
            first_element = bson_pair_rdd.first()
            self.assertIsInstance(first_element, tuple)
            self.assertEqual(2, len(first_element))
        finally:
            try:
                shutil.rmtree(bson_location)
            except Exception:
                pass
