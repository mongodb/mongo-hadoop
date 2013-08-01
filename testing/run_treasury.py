#!/bin/env python

import re
import logging
import tempfile
import shutil
import unittest
import pymongo
import mongo_manager
import subprocess
import os
import shutil
from datetime import timedelta
import time
import json

import sys
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tools'))
from bson_splitter import split_bson

CLEANUP_TMP=os.environ.get('CLEANUP_TMP', True)
HADOOP_HOME=os.environ['HADOOP_HOME']
HADOOP_RELEASE=os.environ.get('HADOOP_RELEASE',None)
AWS_SECRET=os.environ.get('AWS_SECRET',None) 
AWS_ACCESSKEY=os.environ.get('AWS_ACCESSKEY',None) 
TEMPDIR=os.environ.get('TEMPDIR','/tmp')
USE_ASSEMBLY=os.environ.get('USE_ASSEMBLY', True)
num_runs = 0

if not os.path.isdir(TEMPDIR):
    os.makedirs(TEMPDIR)


#declare -a job_args
#cd ..
VERSION_SUFFIX = "1.1.0"

version_buildtarget =\
    {"0.22" : "0.22.0",
     "1.0" : "1.0.4",
     "1.1" : "1.1.2",
     "cdh4" : "cdh4.3.0",
     "0.20" : "0.20.205.0",
     "0.23" : "0.23.1",
     "cdh3" :"cdh3u3"}

def generate_jar_name(prefix, version_suffix):
    if HADOOP_RELEASE:
        for k, v in version_buildtarget.iteritems():
            if HADOOP_RELEASE.startswith(k):
                return prefix + "_" + v + "-" + version_suffix + ".jar"
    else:
        return prefix + "*.jar"

treasury_jar_name = generate_jar_name("treasury-example", VERSION_SUFFIX);
if USE_ASSEMBLY is True:
    streaming_jar_name = 'mongo-hadoop-streaming-assembly-' + VERSION_SUFFIX + ".jar"
else:
    streaming_jar_name = generate_jar_name("mongo-hadoop-streaming", VERSION_SUFFIX);

# result set for sanity check#{{{
check_results = [ { "_id": 1990, "count": 250, "avg": 8.552400000000002, "sum": 2138.1000000000004 }, 
                  { "_id": 1991, "count": 250, "avg": 7.8623600000000025, "sum": 1965.5900000000006 },
                  { "_id": 1992, "count": 251, "avg": 7.008844621513946, "sum": 1759.2200000000005 },
                  { "_id": 1993, "count": 250, "avg": 5.866279999999999, "sum": 1466.5699999999997 },
                  { "_id": 1994, "count": 249, "avg": 7.085180722891565, "sum": 1764.2099999999996 },
                  { "_id": 1995, "count": 250, "avg": 6.573920000000002, "sum": 1643.4800000000005 },
                  { "_id": 1996, "count": 252, "avg": 6.443531746031742, "sum": 1623.769999999999 },
                  { "_id": 1997, "count": 250, "avg": 6.353959999999992, "sum": 1588.489999999998 },
                  { "_id": 1998, "count": 250, "avg": 5.262879999999994, "sum": 1315.7199999999984 },
                  { "_id": 1999, "count": 251, "avg": 5.646135458167332, "sum": 1417.1800000000003 },
                  { "_id": 2000, "count": 251, "avg": 6.030278884462145, "sum": 1513.5999999999985 },
                  { "_id": 2001, "count": 248, "avg": 5.02068548387097, "sum": 1245.1300000000006 },
                  { "_id": 2002, "count": 250, "avg": 4.61308, "sum": 1153.27 },
                  { "_id": 2003, "count": 250, "avg": 4.013879999999999, "sum": 1003.4699999999997 },
                  { "_id": 2004, "count": 250, "avg": 4.271320000000004, "sum": 1067.8300000000008 },
                  { "_id": 2005, "count": 250, "avg": 4.288880000000001, "sum": 1072.2200000000003 },
                  { "_id": 2006, "count": 250, "avg": 4.7949999999999955, "sum": 1198.7499999999989 },
                  { "_id": 2007, "count": 251, "avg": 4.634661354581674, "sum": 1163.3000000000002 },
                  { "_id": 2008, "count": 251, "avg": 3.6642629482071714, "sum": 919.73 },
                  { "_id": 2009, "count": 250, "avg": 3.2641200000000037, "sum": 816.0300000000009 },
                  { "_id": 2010, "count": 189, "avg": 3.3255026455026435, "sum": 628.5199999999996 } ]#}}}
                             
def compare_results(collection, reference=check_results):
    output = list(collection.find().sort("_id"))
    if len(output) != len(reference):
        print "count is not same", len(output), len(reference)
        print output
        return False
    for i, doc in enumerate(output):
        #round to account for slight changes due to precision in case ops are run in different order.
        if doc['_id'] != reference[i]['_id'] or \
                doc['count'] != reference[i]['count'] or \
                round(doc['avg'], 7) != round(reference[i]['avg'], 7): 
            print "docs do not match", doc, reference[i]
            return False
    return True


MONGO_HADOOP_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOBJAR_PATH=os.path.join(MONGO_HADOOP_ROOT,
    "examples",
    "treasury_yield",
    "target",
    treasury_jar_name)

JSONFILE_PATH=os.path.join(MONGO_HADOOP_ROOT,
    'examples',
    'treasury_yield',
    'src',
    'main',
    'resources',
    'yield_historical_in.json')

STREAMING_JARPATH=os.path.join(MONGO_HADOOP_ROOT,
    "streaming",
    "target",
    streaming_jar_name)
STREAMING_MAPPERPATH=os.path.join(MONGO_HADOOP_ROOT,
    "streaming",
    "examples",
    "treasury",
    "mapper.py")

STREAMING_REDUCERPATH=os.path.join(MONGO_HADOOP_ROOT,
    "streaming",
    "examples",
    "treasury",
    "reducer.py")

DEFAULT_PARAMETERS = {
  "mongo.job.verbose":"true",
  "mongo.job.background":"false",
  #"mongo.input.key":"",
  #"mongo.input.query":"",
  "mongo.job.mapper":"com.mongodb.hadoop.examples.treasury.TreasuryYieldMapper",
  "mongo.job.reducer":"com.mongodb.hadoop.examples.treasury.TreasuryYieldReducer",
  "mongo.job.input.format":"com.mongodb.hadoop.MongoInputFormat",
  "mongo.job.output.format":"com.mongodb.hadoop.MongoOutputFormat",
  "mongo.job.output.key":"org.apache.hadoop.io.IntWritable",
  "mongo.job.output.value":"com.mongodb.hadoop.io.BSONWritable",
  "mongo.job.mapper.output.key":"org.apache.hadoop.io.IntWritable",
  "mongo.job.mapper.output.value":"org.apache.hadoop.io.DoubleWritable",
  #"mongo.job.combiner":"com.mongodb.hadoop.examples.treasury.TreasuryYieldReducer",
  "mongo.job.partitioner":"",
  "mongo.job.sort_comparator":"",
}

DEFAULT_OLD_PARAMETERS = DEFAULT_PARAMETERS.copy()
DEFAULT_OLD_PARAMETERS.update(
        { "mongo.job.mapper": "com.mongodb.hadoop.examples.treasury.TreasuryYieldMapperV2",
          "mongo.job.reducer": "com.mongodb.hadoop.examples.treasury.TreasuryYieldReducerV2",
          "mongo.job.input.format": "com.mongodb.hadoop.mapred.MongoInputFormat",
          "mongo.job.output.format": "com.mongodb.hadoop.mapred.MongoOutputFormat"})

#TODO - too many keyword args here - refactor/simplify.
def runjob(hostname, params, input_collection='mongo_hadoop.yield_historical.in',
           output_collection='mongo_hadoop.yield_historical.out',
           output_hostnames=[],
           readpref="primary",
           input_auth=None,
           output_auth=None,
           className="com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig"):
    cmd = [os.path.join(HADOOP_HOME, "bin", "hadoop")]
    cmd.append("jar")
    cmd.append(JOBJAR_PATH)
    cmd.append(className);

    for key, val in params.items():
        cmd.append("-D")
        cmd.append(key + "=" + val)

    
    #if it's not set, assume that the test is 
    # probably setting it in some other property (e.g. multi collection)
    if input_collection:
        cmd.append("-D")
        if type(input_collection) == type([]):
            input_uri = " ".join('mongodb://%s/%s?readPreference=%s' % (hostname, x, readpref) for x in input_collection)
            input_uri = '"' + input_uri + '"'
        else:
            input_uri = 'mongodb://%s%s/%s?readPreference=%s' % (input_auth + "@" if input_auth else '', hostname, input_collection, readpref) 
        cmd.append("mongo.input.uri=%s" % input_uri)

    cmd.append("-D")
    if not output_hostnames:# just use same as input host name
        cmd.append("mongo.output.uri=mongodb://%s%s/%s" % (output_auth + "@" if output_auth else '', hostname, output_collection))
    else:
        output_uris = ['mongodb://%s%s/%s' % (output_auth + "@" if output_auth else '', host, output_collection) for host in output_hostnames]
        cmd.append("mongo.output.uri=\"" + ' '.join(output_uris) + "\"")

    print cmd
    logging.info(cmd)
    subprocess.call(' '.join(cmd), shell=True)

def runbsonjob(input_path, params, hostname,
               output_collection='mongo_hadoop.yield_historical.out',
               output_hostnames=[],
               className="com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig"):
    cmd = [os.path.join(HADOOP_HOME, "bin", "hadoop")]
    cmd.append("jar")
    cmd.append(JOBJAR_PATH)
    cmd.append(className)
    print cmd

    for key, val in params.items():
        cmd.append("-D")
        cmd.append(key + "=" + val)

    cmd.append("-D")
    cmd.append("mapred.input.dir=%s" % (input_path))
    cmd.append("-D")
    if not output_hostnames:# just use same as input host name
        cmd.append("mongo.output.uri=mongodb://%s/%s" % (hostname, output_collection))
    else:
        output_uris = ['mongodb://%s/%s' % (host, output_collection) for host in output_hostnames]
        cmd.append("mongo.output.uri=\"" + ' '.join(output_uris) + "\"")

    print cmd
    subprocess.call(' '.join(cmd), shell=True)
    

def runstreamingjob(hostname, params, input_collection='mongo_hadoop.yield_historical.in',
           output_collection='mongo_hadoop.yield_historical.out',
           readpref="primary",
           input_auth=None,
           output_auth=None, 
           inputpath='file://' + os.path.join(TEMPDIR, 'in'),
           outputpath='file://' + os.path.join(TEMPDIR, 'out'),
           inputformat='com.mongodb.hadoop.mapred.MongoInputFormat',
           outputformat='com.mongodb.hadoop.mapred.MongoOutputFormat'):

    cmd = [os.path.join(HADOOP_HOME, "bin", "hadoop")]
    print cmd
    if HADOOP_RELEASE.startswith('cdh3') or HADOOP_RELEASE.startswith('1.1'):
        #Special case for cdh3, as it uses non-default location.
        cmd += ['jar','$HADOOP_HOME/contrib/streaming/hadoop-streaming*']
    else:
        cmd += ['jar','$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*']
    cmd += ["-libjars", STREAMING_JARPATH]
    cmd += ["-input", inputpath]
    cmd += ["-output", outputpath]
    cmd += ["-inputformat",inputformat]
    cmd += ["-outputformat",outputformat]
    cmd += ["-io", 'mongodb']
    input_uri = 'mongodb://%s%s/%s?readPreference=%s' % (input_auth + "@" if input_auth else '', hostname, input_collection, readpref) 
    cmd += ['-jobconf', "mongo.input.uri=%s" % input_uri]
    output_uri = "mongo.output.uri=mongodb://%s%s/%s" % (output_auth + "@" if output_auth else '', hostname, output_collection) 
    cmd += ['-jobconf', output_uri]
    cmd += ['-jobconf', 'stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver']

    cmd += ['-mapper', STREAMING_MAPPERPATH]
    cmd += ['-reducer', STREAMING_REDUCERPATH]

    for key, val in params.items():
        cmd.append("-jobconf")
        cmd.append(key + "=" + val)

    subprocess.call(' '.join(cmd), shell=True)


class Standalone(unittest.TestCase):
    noauth=True

    @classmethod
    def setUpClass(self):
        global num_runs
        self.homedir = "standalone1_" + str(num_runs)
        self.server = mongo_manager.StandaloneManager(home=os.path.join(TEMPDIR,self.homedir))
        self.server_hostname = self.server.start_server(fresh=True,noauth=self.noauth)
        self.server.connection().drop_database('mongo_hadoop')
        mongo_manager.mongo_import(self.server_hostname,
                                   "mongo_hadoop",
                                   "yield_historical.in",
                                   JSONFILE_PATH)
        num_runs += 1

    def setUp(self):
        self.server.connection()['mongo_hadoop']['yield_historical.out'].drop()

    def tearDown(self):
        print "Standalone Teardown"
        logging.info("Standalone Teardown")
        pass


    @classmethod
    def tearDownClass(self):
        print "Standalone Teardown: killing mongod"
        logging.info("Standalone Teardown: killing mongod")
        self.server.kill_all_members()
        shutil.rmtree(os.path.join(TEMPDIR,self.homedir))
        time.sleep(1)

class TestBasic(Standalone):

    def test_treasury(self):
        logging.info("testing basic input source")
        PARAMS = DEFAULT_PARAMETERS.copy()
        PARAMS['mongo.input.notimeout'] = "true"
        runjob(self.server_hostname, PARAMS)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

class TestBasicMulti(Standalone):

    def test_treasury_json_config(self):
        mongo_manager.mongo_import(self.server_hostname,
                                   "mongo_hadoop",
                                   "yield_historical.in3",
                                   JSONFILE_PATH)
        PARAMS = DEFAULT_PARAMETERS.copy()
        PARAMS['mongo.splitter.class'] = "com.mongodb.hadoop.splitter.MultiMongoCollectionSplitter"
        collection_settings = [{"mongo.input.uri":"mongodb://%s/mongo_hadoop.yield_historical.in" % self.server_hostname,
                                "query":{"dayOfWeek":"FRIDAY"},
                                "mongo.splitter.class":"com.mongodb.hadoop.splitter.SingleMongoSplitter",
                                "mongo.input.split.use_range_queries":True,
                                "mongo.input.notimeout":True},
                               {"mongo.input.uri":"mongodb://%s/mongo_hadoop.yield_historical.in3" % self.server_hostname,
                                "mongo.input.split.use_range_queries":True,
                                "mongo.input.notimeout":True} ]
        #we need to escape this for the shell
        PARAMS["mongo.input.multi_uri.json"] = '"' + re.sub('"','\\"', json.dumps(collection_settings) ) + '"'
        runjob(self.server_hostname, PARAMS, input_collection=None)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        print(list(out_col.find()))

    def test_treasury(self):
        logging.info("testing multiple collection support.")
        mongo_manager.mongo_import(self.server_hostname,
                                   "mongo_hadoop",
                                   "yield_historical.in2",
                                   JSONFILE_PATH)
        PARAMS = DEFAULT_PARAMETERS.copy()
        PARAMS['mongo.splitter.class'] = "com.mongodb.hadoop.splitter.MultiMongoCollectionSplitter"
        runjob(self.server_hostname, PARAMS,
                input_collection=['mongo_hadoop.yield_historical.in', \
                                  'mongo_hadoop.yield_historical.in2'])
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        reference_doubled = [{"_id":x['_id'],
                              "count":x['count'] * 2,
                              "avg": (x['sum']*2) / (x['count']*2),
                              "sum": x['sum']*2} for x in check_results]
        self.assertTrue(compare_results(out_col, reference_doubled))
        print list(out_col.find())

class BaseShardedTest(unittest.TestCase):
    noauth=True

    @classmethod
    def setUpClass(self):
        time.sleep(5)
        global num_runs

        self.shard1 = mongo_manager.ReplicaSetManager(home=os.path.join(TEMPDIR, "rs0_" + str(num_runs)),
                with_arbiter=True,
                num_members=3, noauth=self.noauth)
        self.shard1.start_set(fresh=True)
        self.shard2 = mongo_manager.ReplicaSetManager(home=os.path.join(TEMPDIR, "rs1_" + str(num_runs)),
                with_arbiter=True,
                num_members=3, noauth=self.noauth)
        self.shard2.start_set(fresh=True)
        self.configdb = mongo_manager.StandaloneManager(home=os.path.join(TEMPDIR, 'config_db_' + str(num_runs)))
        self.confighost = self.configdb.start_server(fresh=True,noauth=self.noauth)

        self.mongos = mongo_manager.MongosManager(home=os.path.join(TEMPDIR, 'mongos_' + str(num_runs)))
        self.mongos_hostname = self.mongos.start_mongos(self.confighost,
                [h.get_shard_string() for h in (self.shard1,self.shard2)],
                noauth=self.noauth, fresh=True, addShards=True)

        self.mongos2 = mongo_manager.MongosManager(home=os.path.join(TEMPDIR, 'mongos2_' + str(num_runs)))
        self.mongos2_hostname = self.mongos2.start_mongos(self.confighost,
                [h.get_shard_string() for h in (self.shard1,self.shard2)],
                noauth=self.noauth, fresh=True, addShards=False)

        self.mongos_connection = self.mongos.connection()
        self.mongos2_connection = self.mongos2.connection()
        self.mongos_connection.drop_database('mongo_hadoop')
        mongo_manager.mongo_import(self.mongos_hostname,
                                   "mongo_hadoop",
                                   "yield_historical.in",
                                   JSONFILE_PATH)
        mongos_admindb = self.mongos_connection['admin']
        mongos_admindb.command("enablesharding", "mongo_hadoop")
        self.homedirs = [x + str(num_runs) for x in ("rs0_", "rs1_", "config_db_", "mongos_", "mongos2_")]
        num_runs += 1

        #turn off the balancer
        self.mongos_connection['config'].settings.update({ "_id": "balancer" }, { '$set' : { 'stopped': True } }, True );
        mongos_admindb.command("shardCollection",
                "mongo_hadoop.yield_historical.in",
                key={"_id":1})

        testcoll = self.mongos_connection['mongo_hadoop']['yield_historical.in']

        for chunkpos in [2000, 3000, 1000, 500, 4000, 750, 250, 100, 3500, 2500, 2250, 1750]:
            mongos_admindb.command("split", "mongo_hadoop.yield_historical.in",
                    middle={"_id":testcoll.find().sort("_id", 1).skip(chunkpos).limit(1)[0]['_id']})

        ms_config = self.mongos_connection['config']
        shards = list(ms_config.shards.find())
        numchunks = ms_config.chunks.count()
        chunk_source = ms_config.chunks.find_one()['shard']
        logging.info("chunk source", chunk_source)
        chunk_dest = [s['_id'] for s in shards if s['_id'] != chunk_source][0]
        logging.info("chunk dest", chunk_dest)
        #shuffle chunks around
        for i in xrange(0, numchunks/2):
            chunk_to_move = ms_config.chunks.find_one({"shard":chunk_source})
            logging.info("moving", chunk_to_move, "from", chunk_source, "to", chunk_dest)
            try:
                mongos_admindb.command("moveChunk", "mongo_hadoop.yield_historical.in", find=chunk_to_move['min'], to=chunk_dest);
            except Exception, e:
                print e

        time.sleep(10)

    def setUp(self):
        self.mongos_connection['mongo_hadoop']['yield_historical.out'].drop()

    @classmethod
    def tearDownClass(self):
        print "BaseShardedTest: killing sharded servers!"
        logging.info("BaseShardedTest: killing sharded servers!")
        self.mongos.kill_all_members(sig=9)
        self.mongos2.kill_all_members(sig=9)
        self.shard1.kill_all_members(sig=9)
        self.shard2.kill_all_members(sig=9)
        self.configdb.kill_all_members(sig=9)
        if CLEANUP_TMP != 'false':
            for dirname in self.homedirs:
                shutil.rmtree(os.path.join(TEMPDIR,dirname))

        time.sleep(7.5)


class TestSharded(BaseShardedTest):
    #run a simple job against a sharded cluster, going against the mongos directly

    def test_treasury(self):
        logging.info("Testing basic mongos")
        runjob(self.mongos_hostname, DEFAULT_PARAMETERS)
        out_col = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

    def test_treasury_multi_mongos_input(self):
        logging.info("Testing sharded cluster input with multiplexed mongos INPUTS")
        PARAMS = DEFAULT_PARAMETERS.copy()
        PARAMS['mongo.input.mongos_hosts'] = '"' + ' '.join([self.mongos_hostname, self.mongos2_hostname])+ '"'
        runjob(self.mongos_hostname, PARAMS)
        out_col = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

    def test_treasury_multi_mongos(self):
        logging.info("Testing sharded cluster input with multiplexed mongos OUTPUTS")
        logging.info("before")
        logging.info(self.mongos_connection['admin'].command("serverStatus")['opcounters'])
        logging.info(self.mongos2_connection['admin'].command("serverStatus")['opcounters'])
        runjob(self.mongos_hostname, DEFAULT_PARAMETERS, output_hostnames=[self.mongos_hostname, self.mongos2_hostname])
        out_col = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        logging.info("after")
        logging.info(self.mongos_connection['admin'].command("serverStatus")['opcounters'])
        logging.info(self.mongos2_connection['admin'].command("serverStatus")['opcounters'])
        self.assertTrue(compare_results(out_col))

class TestShardedGTE_LT(BaseShardedTest):

    def test_gte_lt(self):
        logging.info("Testing sharded cluster input with gt/lt query formats")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS['mongo.input.split.use_range_queries'] = 'true'

        shard1db = pymongo.Connection(self.shard1.get_primary()[0])['mongo_hadoop']
        shard2db = pymongo.Connection(self.shard2.get_primary()[0])['mongo_hadoop']
        runjob(self.mongos_hostname, PARAMETERS)
        out_col = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

        PARAMETERS['mongo.input.query'] = '{"_id":{"\$gt":{"\$date":1182470400000}}}'
        out_col.drop()
        runjob(self.mongos_hostname, PARAMETERS)
        #Make sure that this fails when rangequery is used with a query that conflicts
        self.assertEqual(out_col.count(), 0)

class TestShardedNoMongos(BaseShardedTest):
    #run a simple job against a sharded cluster, going directly to shards (bypass mongos)

    def test_treasury(self):
        logging.info("Testing sharded cluster input source, targeting shards directly")
        #PARAMETERS = DEFAULT_PARAMETERS.copy()
        #PARAMETERS['mongo.input.split.read_shard_chunks'] = 'true'
        #logging.info("running job against shards directly")
        #runjob(self.mongos_hostname, PARAMETERS)
        #out_col = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        #self.assertTrue(compare_results(out_col))

        self.mongos_connection['mongo_hadoop']['yield_historical.out'].drop()

        #HADOOP61 - simulate a failed migration by having some docs from one chunk
        #also exist on another shard who does not own that chunk (duplicates)
        ms_config = self.mongos_connection['config']

        chunk_to_duplicate = ms_config.chunks.find_one({"shard":self.shard1.name})
        logging.info("duplicating chunk", chunk_to_duplicate)
        chunk_query = {"_id":{"$gte":chunk_to_duplicate['min']['_id'], "$lt": chunk_to_duplicate['max']['_id']}}
        data_to_duplicate = list(self.mongos_connection['mongo_hadoop']['yield_historical.in'].find(chunk_query))
        destination = pymongo.Connection(self.shard2.get_primary()[0])
        for doc in data_to_duplicate:
            #logging.info(doc['_id'], "was on shard ", self.shard1.name, "now on ", self.shard2.name)
            #print "inserting", doc
            destination['mongo_hadoop']['yield_historical.in'].insert(doc, safe=True)
        
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS['mongo.input.split.allow_read_from_secondaries'] = 'true'
        PARAMETERS['mongo.input.split.read_from_shards'] = 'true'
        PARAMETERS['mongo.input.split.read_shard_chunks'] = 'false'
        runjob(self.mongos_hostname, PARAMETERS, readpref="secondary")

        out_col2 = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        self.assertFalse(compare_results(out_col2))
        self.mongos_connection['mongo_hadoop']['yield_historical.out'].drop()

        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS['mongo.input.split.allow_read_from_secondaries'] = 'true'
        PARAMETERS['mongo.input.split.read_from_shards'] = 'true'
        PARAMETERS['mongo.input.split.read_shard_chunks'] = 'true'
        runjob(self.mongos_hostname, PARAMETERS, readpref="secondary")
        self.assertTrue(compare_results(out_col2))

class TestStreaming(Standalone):

    @unittest.skipIf(HADOOP_RELEASE.startswith('1.0') or HADOOP_RELEASE.startswith('0.20'),
                     'streaming not supported')
    def test_treasury(self):
        logging.info("Testing basic streaming job")
        PARAMETERS = {}
        PARAMETERS['mongo.input.query'] = '{_id:{\$gt:{\$date:883440000000}}}'
        runstreamingjob(self.server_hostname, params=PARAMETERS)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        results = list(out_col.find({},{'_id':1}).sort("_id"))
        self.assertTrue(len(results) == 14)


class TestS3BSON(Standalone):

    @unittest.skipIf(not AWS_ACCESSKEY or not AWS_SECRET, 'AWS credentials not provided')
    def test_treasury(self):
        logging.info("Testing static bson on S3 filesystem")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["mongo.job.input.format"] = "com.mongodb.hadoop.BSONFileInputFormat"
        PARAMETERS["mapred.max.split.size"] = '200000'
        PARAMETERS["fs.s3.awsAccessKeyId"] = AWS_ACCESSKEY
        PARAMETERS["fs.s3.awsSecretAccessKey"] = AWS_SECRET

        #fs.s3.awsAccessKeyId or fs.s3.awsSecretAccessKey properties (respectively).
        s3URL = "s3n://%s:%s@mongo-test-data/yield_historical.in.bson" % (AWS_ACCESSKEY, AWS_SECRET)
        runbsonjob(s3URL, PARAMETERS, self.server_hostname)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

class TestStaticBSON(Standalone):

    def setUp(self):
        super(TestStaticBSON, self).setUp();
        self.temp_outdir = tempfile.mkdtemp(prefix='hadooptest_', dir=TEMPDIR)
        mongo_manager.mongo_dump(self.server_hostname, "mongo_hadoop",
                                   "yield_historical.in", self.temp_outdir)
        

    def tearDown(self):
        logging.info("TestStaticBSON teardown")
        print "TestStaticBSON teardown"
        super(TestStaticBSON, self).tearDown();
        print "removing static bson test dir"
        shutil.rmtree(self.temp_outdir)


    @unittest.skipIf(HADOOP_RELEASE.startswith('1.0') or HADOOP_RELEASE.startswith('0.20'),
                     'streaming not supported')
    def test_streaming_static(self):
        logging.info("Testing streaming static bson")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["mapred.max.split.size"] = '200000'
        inputpath = os.path.join("file://" + self.temp_outdir, "mongo_hadoop","yield_historical.in.bson")
        runstreamingjob(self.server_hostname,
                        inputformat="com.mongodb.hadoop.mapred.BSONFileInputFormat",
                        inputpath=inputpath,
                        params=PARAMETERS)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

    @unittest.skipIf(HADOOP_RELEASE.startswith('1.0') or HADOOP_RELEASE.startswith('0.20'),
                     'streaming not supported')
    def test_streaming_staticout(self):
        logging.info("testing bson output from streaming job")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["bson.split.read_splits"] = 'false'
        PARAMETERS["bson.output.build_splits"] = 'false'
        PARAMETERS["mapred.max.split.size"] = '100000'

        PARAMETERS["mapred.output.file"]= "file://" + os.path.join(TEMPDIR,'BLAH.bson')
        inputpath = os.path.join("file://" + self.temp_outdir, "mongo_hadoop","yield_historical.in.bson")
        runstreamingjob(self.server_hostname,
                        inputformat="com.mongodb.hadoop.mapred.BSONFileInputFormat",
                        outputformat="com.mongodb.hadoop.mapred.BSONFileOutputFormat",
                        inputpath=inputpath,
                        params=PARAMETERS)
        #out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        #self.assertTrue(compare_results(out_col))

    @unittest.skipIf(not AWS_ACCESSKEY or not AWS_SECRET, 'AWS credentials not provided')
    @unittest.skipIf(HADOOP_RELEASE.startswith('1.0') or HADOOP_RELEASE.startswith('0.20'),
                     'streaming not supported')
    def test_streaming_s3_static(self):
        logging.info("Testing streaming static bson on s3")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["mapred.max.split.size"] = '200000'
        PARAMETERS["fs.s3.awsAccessKeyId"] = AWS_ACCESSKEY
        PARAMETERS["fs.s3.awsSecretAccessKey"] = AWS_SECRET
        inputpath = "s3n://%s:%s@mongo-test-data/yield_historical.in.bson" % (AWS_ACCESSKEY, AWS_SECRET)
        runstreamingjob(self.server_hostname,
                        inputformat="com.mongodb.hadoop.mapred.BSONFileInputFormat",
                        inputpath=inputpath,
                        params=PARAMETERS)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))


    def test_treasury_directory(self):
        logging.info("Testing bsoninput from directory path")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["mongo.job.input.format"] = "com.mongodb.hadoop.BSONFileInputFormat"
        PARAMETERS["mapred.max.split.size"] = '200000'
        PARAMETERS["bson.pathfilter.class"] = 'com.mongodb.hadoop.BSONPathFilter'
        logging.info(PARAMETERS)
        runbsonjob(os.path.join("file://" + self.temp_outdir, "mongo_hadoop"), PARAMETERS, self.server_hostname)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

    def test_treasury_directory_out(self):
        logging.info("Testing bsoninput from directory path")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["mongo.job.input.format"] = "com.mongodb.hadoop.BSONFileInputFormat"
        PARAMETERS["mongo.job.output.format"] = "com.mongodb.hadoop.BSONFileOutputFormat"
        PARAMETERS["mapred.output.dir"] = os.path.join("file://" + self.temp_outdir, "mongo_hadoop_out")
        PARAMETERS["mapred.max.split.size"] = '200000'
        PARAMETERS["bson.pathfilter.class"] = 'com.mongodb.hadoop.BSONPathFilter'
        logging.info(PARAMETERS)
        runbsonjob(os.path.join("file://" + self.temp_outdir, "mongo_hadoop"), PARAMETERS, self.server_hostname)

        paths = os.listdir(os.path.join(self.temp_outdir,'mongo_hadoop_out'))
        for p in paths:
            fullpath = os.path.join(self.temp_outdir, "mongo_hadoop_out", p)
            if not fullpath.endswith('.bson'):
                continue
            mongo_manager.mongo_restore(self.server_hostname, "mongo_hadoop", "yield_historical.out",fullpath)

        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))


    def test_treasury_onesplit(self):
        logging.info("Testing bsoninput with one split")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["mongo.job.input.format"] = "com.mongodb.hadoop.BSONFileInputFormat"
        PARAMETERS["bson.split.read_splits"] = 'false'
        logging.info(PARAMETERS)
        runbsonjob(os.path.join("file://" + self.temp_outdir, "mongo_hadoop","yield_historical.in.bson"), PARAMETERS, self.server_hostname)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))
        #runjob(self.server_hostname, DEFAULT_PARAMETERS)


    def test_treasury(self):
        logging.info("Testing bsoninput with no splits")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["mongo.job.input.format"] = "com.mongodb.hadoop.BSONFileInputFormat"
        PARAMETERS["mapred.max.split.size"] = '200000'
        logging.info(PARAMETERS)
        runbsonjob(os.path.join("file://" + self.temp_outdir, "mongo_hadoop","yield_historical.in.bson"), PARAMETERS, self.server_hostname)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))
        #runjob(self.server_hostname, DEFAULT_PARAMETERS)

    def test_prebuilt_splits(self):
        logging.info("Testing bsoninput with pre-built splits")
        #make sure we can do the right thing when the splits are
        #provided by some other tool (e.g. python script)
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["mongo.job.input.format"] = "com.mongodb.hadoop.BSONFileInputFormat"
        split_bson(os.path.join(self.temp_outdir, "mongo_hadoop","yield_historical.in.bson"))
        runbsonjob(os.path.join("file://" + self.temp_outdir, "mongo_hadoop","yield_historical.in.bson"), PARAMETERS, self.server_hostname)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

class TestShardedAuth(BaseShardedTest):
    noauth=False

    def test_treasury(self):
        logging.info("Testing sharding with authentication on")
        self.mongos_connection['config'].add_user("test_user","test_pw")
        self.mongos_connection['mongo_hadoop'].add_user("test_user","test_pw")
        self.mongos_connection['admin'].add_user("test_user","test_pw")

        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS['mongo.input.split.read_shard_chunks'] = 'true'
        authuri = "mongodb://%s:%s@%s/%s" % ('test_user', 'test_pw', self.mongos_hostname, 'config')
        PARAMETERS['mongo.auth.uri'] = authuri
        runjob(self.mongos_hostname, PARAMETERS, readpref="secondary", input_auth="test_user:test_pw",
            output_auth="test_user:test_pw")
        admindb = self.mongos_connection['admin']
        admindb.authenticate("test_user", "test_pw")
        out_col2 = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        #now with credentials, it should work
        self.assertTrue(compare_results(out_col2))

        PARAMETERS = DEFAULT_PARAMETERS.copy()
        #PARAMETERS['mongo.input.split.read_from_shards'] = 'true'
        PARAMETERS['mongo.input.split.read_shard_chunks'] = 'true'
        #PARAMETERS['mongo.input.split.allow_read_from_secondaries'] = 'true'
        PARAMETERS['mongo.auth.uri'] = authuri
        runjob(self.mongos_hostname, PARAMETERS, readpref="secondary",input_auth="test_user:test_pw",
            output_hostnames=[self.mongos_hostname, self.mongos_hostname], output_auth="test_user:test_pw")
        admindb = self.mongos_connection['admin']
        admindb.authenticate("test_user", "test_pw")
        out_col2 = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        #now with credentials, it should work
        self.assertTrue(compare_results(out_col2))

class TestShardedWithQuery(BaseShardedTest):

    def test_treasury(self):
        logging.info("Testing queried input against sharded cluster")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        #Only use a subset of dates
        PARAMETERS['mongo.input.query'] = '{_id:{\$gte:{\$date:883440000000}}}'
        runjob(self.mongos_hostname, PARAMETERS)
        out_col = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        results = list(out_col.find({},{'_id':1}).sort("_id"))
        self.assertTrue(len(results) == 14)


class TestStandaloneAuth(TestBasic):
    noauth=False

    def test_treasury(self):
        logging.info("Testing standalone with authentication on")
        x = self.server.connection()['admin'].add_user("test_user","test_pw", roles=["clusterAdmin", "readWriteAnyDatabase"])
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS['mongo.auth.uri'] = 'mongodb://%s:%s@%s/admin' % ('test_user', 'test_pw', self.server_hostname) 
        runjob('test_user:test_pw@' + self.server_hostname, PARAMETERS)

        server_connection = self.server.connection()
        server_connection['admin'].authenticate("test_user","test_pw")
        out_col2 = server_connection['mongo_hadoop']['yield_historical.out']
        #now with credentials, it should work
        self.assertTrue(compare_results(out_col2))


class TestStandaloneWithQuery(Standalone):

    def test_treasury(self):
        logging.info("Testing standalone with query")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS['mongo.input.query'] = '{_id:{\$gt:{\$date:883440000000}}}'
        runjob(self.server_hostname, PARAMETERS)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        results = list(out_col.find({},{'_id':1}).sort("_id"))
        self.assertTrue(len(results) == 14)


class TestUpdateWritable(Standalone):

    def test_treasury(self):
        logging.info("Testing UpdateWriteable against standalone server")
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS["mongo.job.reducer"] = "com.mongodb.hadoop.examples.treasury.TreasuryYieldUpdateReducer"
        runjob(self.server_hostname, PARAMETERS)
        #run it again.
        runjob(self.server_hostname, PARAMETERS)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        results = list(out_col.find({}).sort("_id"))
        for r in results:
            logging.info("verifying update for", r.get("_id", None))
            self.assertEqual(len(r.get('calculatedAt', [])), 2)
            self.assertEqual(r.get('numCalculations', 0), 2)

class TestOldMRApi(Standalone):

    def test_treasury(self):
        logging.info("Testing OLD Mapreduce API against standalone server")
        runjob(self.server_hostname, DEFAULT_OLD_PARAMETERS, className="com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfigV2")
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

    def test_treasury_query(self):
        logging.info("Testing OLD Mapreduce API against standalone server with query")
        PARAMETERS = DEFAULT_OLD_PARAMETERS.copy()
        PARAMETERS['mongo.input.query'] = '{_id:{\$gte:{\$date:883440000000}}}'
        runjob(self.server_hostname, PARAMETERS, className="com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfigV2")
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        results = list(out_col.find({},{'_id':1}).sort("_id"))
        self.assertTrue(len(results) == 14)


if __name__ == '__main__':
    testtreasury()
