#!/bin/env python
import unittest
import pymongo
import mongo_manager
import subprocess
import os
from datetime import timedelta
import time
HADOOP_HOME=os.environ['HADOOP_HOME']
#declare -a job_args
#cd ..

# result set for sanity check#{{{
check_results = [ { "_id": 1990, "value": 8.552400000000002 },
                  { "_id": 1991, "value": 7.8623600000000025 },
                  { "_id": 1992, "value": 7.008844621513946 },
                  { "_id": 1993, "value": 5.866279999999999 },
                  { "_id": 1994, "value": 7.085180722891565 },
                  { "_id": 1995, "value": 6.573920000000002 },
                  { "_id": 1996, "value": 6.443531746031742 },
                  { "_id": 1997, "value": 6.353959999999992 },
                  { "_id": 1998, "value": 5.262879999999994 },
                  { "_id": 1999, "value": 5.646135458167332 },
                  { "_id": 2000, "value": 6.030278884462145 },
                  { "_id": 2001, "value": 5.02068548387097 },
                  { "_id": 2002, "value": 4.61308 },
                  { "_id": 2003, "value": 4.013879999999999 },
                  { "_id": 2004, "value": 4.271320000000004 },
                  { "_id": 2005, "value": 4.288880000000001 },
                  { "_id": 2006, "value": 4.7949999999999955 },
                  { "_id": 2007, "value": 4.634661354581674 },
                  { "_id": 2008, "value": 3.6642629482071714 },
                  { "_id": 2009, "value": 3.2641200000000037 },
                  { "_id": 2010, "value": 3.3255026455026435 }]#}}}

def compare_results(collection):
    output = list(collection.find().sort("_id"))
    if len(output) != len(check_results):
        print "count is not same", len(output), len(check_results)
        return False
    for i, doc in enumerate(output):
        if doc['_id'] != check_results[i]['_id'] or round(doc['value'], 7) != round(check_results[i]['value'], 7): 
            print "docs do not match", doc, check_results[i]
    return True


MONGO_HADOOP_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOBJAR_PATH=os.path.join(MONGO_HADOOP_ROOT,
    "examples",
    "treasury_yield",
    "target",
    "treasury-example_*.jar")
JSONFILE_PATH=os.path.join(MONGO_HADOOP_ROOT,
    'examples',
    'treasury_yield',
    'src',
    'main',
    'resources',
    'yield_historical_in.json')

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
  "mongo.job.output.value":"org.apache.hadoop.io.DoubleWritable",
  "mongo.job.mapper.output.key":"org.apache.hadoop.io.IntWritable",
  "mongo.job.mapper.output.value":"org.apache.hadoop.io.DoubleWritable",
  #"mongo.job.combiner":"com.mongodb.hadoop.examples.treasury.TreasuryYieldReducer",
  "mongo.job.partitioner":"",
  "mongo.job.sort_comparator":"",
}

def runjob(hostname, params, input_collection='mongo_hadoop.yield_historical.in',
           output_collection='mongo_hadoop.yield_historical.out', readpref="primary"):
    cmd = [os.path.join(HADOOP_HOME, "bin", "hadoop")]
    cmd.append("jar")
    cmd.append(JOBJAR_PATH)

    for key, val in params.items():
        cmd.append("-D")
        cmd.append(key + "=" + val)

    cmd.append("-D")
    cmd.append("mongo.input.uri=mongodb://%s/%s?readPreference=%s" % (hostname, input_collection, readpref))
    cmd.append("-D")
    cmd.append("mongo.output.uri=mongodb://%s/%s" % (hostname, output_collection))

    subprocess.call(' '.join(cmd), shell=True)


class TestBasic(unittest.TestCase):
    
    def setUp(self):
        self.server = mongo_manager.StandaloneManager(home="/tmp/standalone1")  
        self.server_hostname = self.server.start_server(fresh=True)
        self.server.connection().drop_database('mongo_hadoop')
        mongo_manager.mongo_import(self.server_hostname,
                                   "mongo_hadoop",
                                   "yield_historical.in",
                                   JSONFILE_PATH)
        print "server is ready."

    def test_treasury(self):
        runjob(self.server_hostname, DEFAULT_PARAMETERS)
        out_col = self.server.connection()['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))

    def tearDown(self):
        print "killing it!"
        #self.server.kill_all_members()




class BaseShardedTest(unittest.TestCase):

    def setUp(self):
        self.shard1 = mongo_manager.ReplicaSetManager(home="/tmp/rs0",
                with_arbiter=True,
                num_members=3)
        self.shard1.start_set(fresh=True)
        self.shard2 = mongo_manager.ReplicaSetManager(home="/tmp/rs1",
                with_arbiter=True,
                num_members=3)
        self.shard2.start_set(fresh=True)
        self.configdb = mongo_manager.StandaloneManager(home="/tmp/config_db")  
        self.confighost = self.configdb.start_server(fresh=True)

        self.mongos = mongo_manager.MongosManager(home="/tmp/mongos")
        self.mongos_hostname = self.mongos.start_mongos(self.confighost,
                [h.get_shard_string() for h in (self.shard1,self.shard2)],
                noauth=False, fresh=True, addShards=True)

        self.mongos_connection = self.mongos.connection()
        self.mongos_connection.drop_database('mongo_hadoop')
        mongo_manager.mongo_import(self.mongos_hostname,
                                   "mongo_hadoop",
                                   "yield_historical.in",
                                   JSONFILE_PATH)
        mongos_admindb = self.mongos_connection['admin']
        mongos_admindb.command("enablesharding", "mongo_hadoop")

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
        print "chunk source", chunk_source
        chunk_dest = [s['_id'] for s in shards if s['_id'] != chunk_source][0]
        print "chunk dest", chunk_dest
        #shuffle chunks around
        for i in xrange(0, numchunks/2):
            chunk_to_move = ms_config.chunks.find_one({"shard":chunk_source})
            print "moving", chunk_to_move, "from", chunk_source, "to", chunk_dest
            try:
                mongos_admindb.command("moveChunk", "mongo_hadoop.yield_historical.in", find=chunk_to_move['min'], to=chunk_dest);
            except Exception, e:
                print e

        time.sleep(5)


    def tearDown(self):
        print "killing servers!"
        #self.mongos.kill_all_members()
        #self.shard1.kill_all_members()
        #self.shard2.kill_all_members()
        #self.configdb.kill_all_members()


class TestSharded(BaseShardedTest):
    #run a simple job against a sharded cluster, going against the mongos directly

    def test_treasury(self):
        runjob(self.mongos_hostname, DEFAULT_PARAMETERS)

        out_col = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col))


class TestShardedNoMongos(BaseShardedTest):
    #run a simple job against a sharded cluster, going directly to shards (bypass mongos)

    def test_treasury(self):
        PARAMETERS = DEFAULT_PARAMETERS.copy()
        PARAMETERS['mongo.input.split.read_shard_chunks'] = 'true'
        #print "running job against shards directly"
        #runjob(self.mongos_hostname, PARAMETERS)
        #out_col = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        #self.assertTrue(compare_results(out_col))

        self.mongos_connection['mongo_hadoop']['yield_historical.out'].drop()

        #HADOOP61 - simulate a failed migration by having some docs from one chunk
        #also exist on another shard who does not own that chunk (duplicates)
        ms_config = self.mongos_connection['config']
        chunk_to_duplicate = ms_config.chunks.find_one({"shard":"replset0"})
        print "duplicating chunk", chunk_to_duplicate
        chunk_query = {"_id":{"$gte":chunk_to_duplicate['min']['_id'], "$lt": chunk_to_duplicate['max']['_id']}}
        data_to_duplicate = self.mongos_connection['mongo_hadoop']['yield_historical.in'].find(chunk_query)
        destination = pymongo.Connection(self.shard2.get_primary()[0])
        for doc in data_to_duplicate:
            #print "inserting", doc
            destination['mongo_hadoop']['yield_historical.in'].insert(doc, safe=True)
        
        PARAMETERS['mongo.input.split.allow_read_from_secondaries'] = 'true'
        runjob(self.mongos_hostname, PARAMETERS, readpref="secondary")

        out_col2 = self.mongos_connection['mongo_hadoop']['yield_historical.out']
        self.assertTrue(compare_results(out_col2))


def testtreasury():
    runjob('localhost:4007')

if __name__ == '__main__': testtreasury()
