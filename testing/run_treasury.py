#!/bin/env python
import unittest
import mongo_manager
import subprocess
import os
import time
HADOOP_HOME=os.environ['HADOOP_HOME']
#declare -a job_args
#cd ..

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

parameters = {
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
  "mongo.job.combiner":"com.mongodb.hadoop.examples.treasury.TreasuryYieldReducer",
  "mongo.job.partitioner":"",
  "mongo.job.sort_comparator":"",
}


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
        print "running treasury example test."
        cmd = [os.path.join(HADOOP_HOME, "bin", "hadoop")]
        cmd.append("jar")
        cmd.append(JOBJAR_PATH)
        #cmd.append("com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig")

        for key, val in parameters.items():
            cmd.append("-D")
            cmd.append(key + "=" + val)

        cmd.append("-D")
        cmd.append("mongo.input.uri=mongodb://%s/mongo_hadoop.yield_historical.in" % self.server_hostname)
        cmd.append("-D")
        cmd.append("mongo.output.uri=mongodb://%s/mongo_hadoop.yield_historical.out" % self.server_hostname)

        subprocess.call(' '.join(cmd), shell=True)
        self.assertEqual( self.server.connection()['mongo_hadoop']['yield_historical.out'].count(), 21 )
        print ' '.join(cmd)

    def tearDown(self):
        print "killing it!"
        self.server.kill_all_members()




class TestSharded(unittest.TestCase):

    def setUp(self):
        self.shard1 = mongo_manager.ReplicaSetManager(home="/tmp/rs0", with_arbiter=True, num_members=3)
        self.shard1.start_set(fresh=True)
        self.shard2 = mongo_manager.ReplicaSetManager(home="/tmp/rs1", with_arbiter=True, num_members=3)
        self.shard2.start_set(fresh=True)
        self.configdb = mongo_manager.StandaloneManager(home="/tmp/config_db")  
        self.confighost = self.configdb.start_server(fresh=True)

        self.mongos = mongo_manager.MongosManager(home="/tmp/mongos")
        self.mongos_hostname = self.mongos.start_mongos(self.confighost, [h.get_shard_string() for h in (self.shard1,self.shard2)],
                noauth=False, fresh=True, addShards=True)

        self.mongos_connection = self.mongos.connection()
        self.mongos_connection.drop_database('mongo_hadoop')
        mongo_manager.mongo_import(self.mongos_hostname,
                                   "mongo_hadoop",
                                   "yield_historical.in",
                                   JSONFILE_PATH)
        self.mongos_connection['admin'].command("enablesharding", "mongo_hadoop")
        self.mongos_connection['admin'].command("shardCollection", "mongo_hadoop.yield_historical.in", key={"_id":1})
        self.mongos_connection['admin'].command("split", "mongo_hadoop.yield_historical.in", find={"_id":1})

    def test_treasury(self):
        print "running treasury example test."
        cmd = [os.path.join(HADOOP_HOME, "bin", "hadoop")]
        cmd.append("jar")
        cmd.append(JOBJAR_PATH)

        for key, val in parameters.items():
            cmd.append("-D")
            cmd.append(key + "=" + val)

        cmd.append("-D")
        cmd.append("mongo.input.uri=mongodb://%s/mongo_hadoop.yield_historical.in?readPreference=primary" % self.mongos_hostname)
        cmd.append("-D")
        cmd.append("mongo.output.uri=mongodb://%s/mongo_hadoop.yield_historical.out" % self.mongos_hostname)

        subprocess.call(' '.join(cmd), shell=True)
        self.assertEqual( self.mongos_connection['mongo_hadoop']['yield_historical.out'].count(), 21 )
        print ' '.join(cmd)

    def tearDown(self):
        print "killing servers!"
        self.mongos.kill_all_members()
        self.shard1.kill_all_members()
        self.shard2.kill_all_members()
        self.configdb.kill_all_members()

#def testtreasury():
        #cmd = [os.path.join(HADOOP_HOME, "bin", "hadoop")]
        #cmd.append("jar")
        #cmd.append("/Users/mike/projects/mongo-hadoop/examples/treasury_yield/target/treasury-example_1.0.3-1.1.0-SNAPSHOT.jar")

        #for key, val in parameters.items():
            #cmd.append("-D")
            #cmd.append(key + "=" + val)
#
        #cmd.append("-D")
        #cmd.append("mongo.input.uri=mongodb://localhost:4007/mongo_hadoop.yield_historical.in")
        #cmd.append("-D")
        #cmd.append("mongo.output.uri=mongodb://localhost:4007/mongo_hadoop.yield_historical.out")

        #subprocess.call(cmd)

if __name__ == '__main__': testtreasury()
