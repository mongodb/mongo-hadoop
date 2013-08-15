#! /usr/bin/env python

import unittest
import subprocess
import sys
import os
import time
import socket
import random 

from pymongo import MongoClient

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# get/set the variables representing environment variables:
# * hivePort
# * testDataFile
# * mongoTestURI
# * testHiveTblName
# * testHiveTblSchema
# * testHiveFileType
# * testHiveFieldsDelim
# * testMongoTblName
# * testMongoPath
# * testMongoPName
# * mongoPort
# * verbose
hivePort = int(os.environ.get("HIVE_PORT", 10000))
mongoPort = int(os.environ.get("MONGOD_PORT", 27017))
testDataFile = None
testHiveTblSchema = None
testMongoPath = None
serdeProperties = None
hostname = socket.gethostname()
testHiveTblName = os.environ.get("HIVE_TEST_TABLE", "hive_test")
testHiveFileType = os.environ.get("HIVE_TEST_FILE_TYPE", "textfile")
testHiveFieldsDelim = os.environ.get("HIVE_TEST_DELIM", '\t')
testMongoTblName = os.environ.get("MONGO_TEST_TABLE", "mongo_test")
testMongoPName = os.environ.get("MSH_PACKAGE_NAME", 
                                "com.mongodb.hadoop.hive.MongoStorageHandler")
serdeProperties = os.environ.get("SERDE_PROPERTIES", "''=''")
verbose = bool(os.environ.get("VERBOSE_TESTS", False))

try:
    testDataFile = os.environ.get("TEST_DATA_FILE")
    testHiveTblSchema = os.environ.get("HIVE_TEST_SCHEMA")
    testMongoPath = os.environ.get("MSH_PATH")
    mongoTestURI = os.environ.get("MONGO_TEST_URI")
except KeyError:
    print "You must set ALL these environment variables:"
    print "\t$TEST_DATA_FILE : absolute path of test data file"
    print "\t$HIVE_TEST_SCHEMA : hive schema of TEST_DATA_FILE"
    print "\t$MSH_PATH : absolute path of MongoStorageHandler jar"
    print "\t$MONGO_TEST_URI : URI of mongo collection"
    sys.exit(1)

"""
Some Helper methods to be used in the unit tests to follow
"""
class Helpers:
    """
    Delays execution thread of tests until the process
    represented by 'proc' and hosted at hostname and port
    has started listening
    """
    @staticmethod
    def waitFor(proc, hostname, port):
        trys = 0
        returnCode = proc.poll()
        TIME_OUT = 100
        while returnCode is None and trys < TIME_OUT:
            trys += 1
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((hostname, port))
                return True
            except (IOError, socket.error):
                time.sleep(0.25)
            finally:
                s.close()
            returnCode = proc.poll()
        return False
    
    """
    Start hive server at 'hivePort'
    """
    @staticmethod
    def startHiveServer():
        cmd = ["hive",
               "--service", "hiveserver",
               "-p", str(hivePort)]
        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE)
        Helpers.waitFor(proc, hostname, hivePort)
        return proc.pid
        
    """
    Stops hive server
    """
    @staticmethod
    def stopHiveServer(pid):
        cmd = ["kill", "-9", str(pid)]
        subprocess.call(cmd)

    """
    Drops hive table 'tblName'
    """
    @staticmethod
    def dropTable(client, tblName):
        cmd = ["DROP TABLE", tblName]
        Helpers.executeQuery(client, cmd)

    """
    Creates HDFS-based hive table 'tblName' with schema 'schema',
    delimited by 'delim' of file type 'ftype'
    """
    @staticmethod
    def createHiveTable(client, tblName, schema, delim, ftype):
        cmd = ["CREATE TABLE", tblName, schema,
               "ROW FORMAT DELIMITED",
               "FIELDS TERMINATED BY", Helpers.quote(delim),
               "STORED AS", ftype]
        Helpers.executeQuery(client, cmd)
        
    """
    Loads data into already-created HDFS-based hive table
    """
    @staticmethod
    def loadDataIntoHiveTable(client):
        Helpers.dropTable(client, testHiveTblName)
        Helpers.createHiveTable(client, testHiveTblName,
                                testHiveTblSchema, testHiveFieldsDelim,
                                testHiveFileType)
        # then load data into the new table
        cmd = ["LOAD DATA LOCAL INPATH", Helpers.quote(testDataFile),
               "INTO TABLE", testHiveTblName]
        Helpers.executeQuery(client, cmd)

    """
    Add jar where MongoStorageHandler resides to the hive path  
    """
    @staticmethod
    def addJars(client):
        cmd = ["ADD JAR", testMongoPath]
        Helpers.executeQuery(client, cmd)

    """
    Wrap quotes around a string
    """
    @staticmethod
    def quote(toQuote):
        return "'" + toQuote + "'"

    """
    Load data into a MongoDB-based hive table. You can specify
    if you want to use SerDe properties (via SERDEPROPERTIES) or not.
    """
    @staticmethod
    def loadDataIntoMongoTable(client, withSerDeProps):
        Helpers.addJars(client)
        Helpers.dropTable(client, testMongoTblName)

        # create the MongoDB-based hive table using the MongoStorageHandler
        props = serdeProperties if withSerDeProps else "''=''"
        cmd = ["CREATE TABLE", testMongoTblName, testHiveTblSchema,
               "STORED BY", Helpers.quote(testMongoPName),
               "WITH SERDEPROPERTIES(", props, ")",
               "TBLPROPERTIES ('mongo.uri'=",
               Helpers.quote(mongoTestURI),
               ")"]
        Helpers.executeQuery(client, cmd)

        cmd = ["INSERT OVERWRITE TABLE", testMongoTblName,
               "SELECT * FROM", testHiveTblName]
        Helpers.executeQuery(client, cmd)

    """
    Get all rows from the hive table 'tblName'
    """
    @staticmethod
    def getAllDataFromTable(client, tblName):
        cmd = ["SELECT * FROM", tblName]
        Helpers.executeQuery(client, cmd)
        schema = client.getSchema()
        data = []
        try:
            for line in client.fetchAll():
                data.append(line.split(testHiveFieldsDelim))
        except Thrift.TException, tx:
            pass
        return (schema, data)

    @staticmethod
    def getOneFromCollection(coll):
        return coll.find_one()

    @staticmethod
    def getAllFromCollection(coll):
        return coll.find()

    @staticmethod
    def deleteFromCollection(coll, doc):
        coll.remove(doc)

    @staticmethod
    def dropCollection(coll):
        coll.drop()

    """
    Given a (valid) mongoURI, return the database and collection names in the URI
    """
    @staticmethod
    def getDBAndCollNames(mongoURI):
        return mongoURI[mongoURI.rfind("/")+1:].split(".")

    @staticmethod
    def getCollectionCount(coll):
        return coll.count()

    @staticmethod
    def getTableCount(client, tblName):
        cmd = ["SELECT COUNT(1) FROM ", tblName]
        Helpers.executeQuery(client, cmd)
        count = client.fetchOne()
        return int(count)

    """
    Executes the hive query in the list 'lscmd'
    """
    @staticmethod
    def executeQuery(client, lscmd):
        assert(client != None)
        assert(len(lscmd) >= 1)
        query = " ".join(lscmd)
        if verbose:
            print "executing", query
        client.execute(query)
    
    """
    Prepares a test suite for executing hive queries. 
    """
    @staticmethod
    def setUpClass(cls):
        transport = None
        try:
            # connect to the mongod
            conn = MongoClient(hostname, mongoPort)
            dbName, collName = Helpers.getDBAndCollNames(mongoTestURI)
            
            # start the hive server
            cls.hserverpid = Helpers.startHiveServer()
            print "Successfully started hive server"
            
            ts = TSocket.TSocket(hostname, hivePort)
            transport = TTransport.TBufferedTransport(ts)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            
            client = ThriftHive.Client(protocol)
            transport.open()
            
            cls.transport = transport
            cls.client = client
            cls.mongoc = conn[dbName][collName]
        except Thrift.TException, tx:
            print 'Error: %s' % (tx.message)
            if transport:
                transport.close()
                
    """
    Stops the hive server used in running hive-based tests. Closes the hive transport.
    """
    @staticmethod
    def tearDownClass(cls):
        try:
            cls.transport.close()
            # stop hive server
            Helpers.stopHiveServer(cls.hserverpid)
            print "Successfully stopped hiveserver"
        except Thrift.TException, tx:
            print "Unable to close transport and/or close hiveserver"
            print '%s' % (tx.message)

"""
To test:
1. Test that data loaded from a HDFS-based hive table into MongoDB-based hive table is 
   copied over correctly.
2. Test that deleting an entry in MongoDB also deletes that entry
   in the MongoDB-based hive table mirroring the MongoDB collection.
3. Drop a MongoDB collection that stores data in a MongoDB-based hive table.
   Make sure that the hive table is also emptied.
"""
class TestBasicMongoTable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Helpers.setUpClass(cls)

    def setUp(self):
        try:          
            Helpers.loadDataIntoHiveTable(self.client)
            Helpers.loadDataIntoMongoTable(self.client, False)
        except Thrift.TException, tx:
            print '%s' % (tx.message)

    def tearDown(self):
        try:
            Helpers.dropTable(self.client, testMongoTblName)
            Helpers.dropTable(self.client, testHiveTblName)
        except Thrift.TException, tx:
            print '%s' % (tx.message)

    @classmethod
    def tearDownClass(cls):
        Helpers.tearDownClass(cls) 
        # wait for a number of seconds after shutting the hive server
        # before next suite of tests are run to avoid a
        # 'connection reset by peer' network error
        time.sleep(2)

    def testSameDataInTables(self):
        hiveSchema, hiveData = Helpers.getAllDataFromTable(self.client, testHiveTblName)
        mongoSchema, mongoData = Helpers.getAllDataFromTable(self.client, testMongoTblName)

        self.assertEqual(hiveSchema, mongoSchema)
        self.assertEqual(len(hiveData), len(mongoData))
        for i in range(len(hiveData)):
            self.assertEqual(hiveData[i], mongoData[i])

    def testDeleteReflectData(self):
        mongoSchema, mongoTblData = Helpers.getAllDataFromTable(self.client, testMongoTblName)
        mongoSchema = mongoSchema.fieldSchemas

        l = len(mongoTblData)
        self.assertTrue(l > 0)

        t = mongoTblData[random.randint(0, l-1)]
        toDelete = {}
        for i in range(len(mongoSchema)):
            # add more types as necessary
            if mongoSchema[i].type == "int":
                toDelete[mongoSchema[i].name] = int(t[i])
            elif mongoSchema[i].type == "string":
                toDelete[mongoSchema[i].name] = str(t[i])
            else:
                toDelete[mongoSchema[i].name] = t[i]
                    
        Helpers.deleteFromCollection(self.mongoc, toDelete)

        # get data from table now that the first row has been removed
        mongoSchema, mongoTblData = Helpers.getAllDataFromTable(self.client, testMongoTblName)

        # now make sure that 'toDelete' doesn't exist anymore
        for line in mongoTblData:
            self.assertNotEqual(line, t)
            
    def testDropReflectData(self):
        mongoSchema, mongoTblData = Helpers.getAllDataFromTable(self.client, testMongoTblName)
        self.assertTrue(len(mongoTblData) > 0)

        # now, drop the collection
        Helpers.dropCollection(self.mongoc)

        mongoSchema, mongoTblData = Helpers.getAllDataFromTable(self.client, testMongoTblName)
        self.assertTrue(len(mongoTblData) == 0)

"""
To test:
1. Using more options in SERDEPROPERTIES,
   define 'mongo.columns.mapping', a one-to-one mapping
   between table columns and mongo fields.
   Make sure that the mapping is correctly mirrored.
"""
class TestOptionsMongoTable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Helpers.setUpClass(cls)

    def setUp(self):
        try:          
            Helpers.loadDataIntoHiveTable(self.client)
            Helpers.loadDataIntoMongoTable(self.client, True)
        except Thrift.TException, tx:
            print '%s' % (tx.message)

    def tearDown(self):
        try:
            Helpers.dropTable(self.client, testMongoTblName)
            Helpers.dropTable(self.client, testHiveTblName)
        except Thrift.TException, tx:
            print '%s' % (tx.message)

    @classmethod
    def tearDownClass(cls):
        Helpers.tearDownClass(cls)

    def testMongoMapping(self):
        doc = Helpers.getOneFromCollection(self.mongoc)
        propsSplit = serdeProperties.split("=")
        # make sure that serdeProperties has key-value pairs
        self.assertTrue(len(propsSplit) % 2 == 0)
        
        # now read in the 'mongo.columns.mapping' mapping
        colsMap = None
        propsSplitLen = len(propsSplit)
        for i in range(propsSplitLen):
            entry = propsSplit[i]
            if entry.lower() == "'mongo.columns.mapping'":
                if i-1 < propsSplitLen:
                    colsMap = propsSplit[i+1]
                    break
        
        self.assertIsNotNone(colsMap)
        # first remove '' around colsMap
        self.assertTrue(colsMap[0] == "'" and colsMap[len(colsMap)-1] == "'")
        
        lsMap = [each.strip() for each in colsMap[1:len(colsMap)-1].split(",")]
        docKeys = doc.keys()
        self.assertTrue(set(docKeys) == set(lsMap))
        
    def testLenTable(self):
        collCount = Helpers.getCollectionCount(self.mongoc)
        tableCount = Helpers.getTableCount(self.client, testMongoTblName)
        self.assertTrue(collCount == tableCount)
        
if __name__ == "__main__":
    unittest.main()
