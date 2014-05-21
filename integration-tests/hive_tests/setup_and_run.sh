# !/usr/bin/env bash

## sets some parameters to be used in the 'test' script. Starts required
## daemons (mongod). Runs tests in standalone mode (single-node cluster).
##
## To run these unit tests, you must have the following installed:
## * hive (+ accompanying Python HiveClient and Thrift libraries)
## * hadoop
## * MongoDB 
## * mongo-hadoop-hive-*.jar

# test directory -- CHANGE ME --
export TEST_DIR="/Users/jlee/dev/mongo-hadoop/integration-tests/hive_tests"

# location of data file used in testing
export TEXT_TEST_PATH="$TEST_DIR/../../hive/src/test/resources/test_data.txt"

# Path containing BSON file to be used for testing
export BSON_LOCAL_TEST_PATH="$TEST_DIR/bson_test_files/users.bson"

# directory to transfer "bson_test_files" for use in testing
export BSON_HDFS_TEST_PATH="/user/hive/warehouse/bson_test_files/"

# schema of the data files (bson,text) used in testing
export TEST_SCHEMA="(id INT, name STRING, age INT)"

# Type of text data file. For example, sequencefile, rcfile, textfile
export HDFS_TEST_FILE_TYPE="TEXTFILE"

# path (absolute or relative) where MongoStorageHandler, BSONStorageHandler reside 
# -- CHANGE ME --
export PACKAGE_PATH="/Users/jlee/dev/mongo-hadoop/hive/build/libs/mongo-hadoop-hive-1.2.1-SNAPSHOT-hadoop_2.4.jar"

# Class name of MongoStorageHandler (prefixed by package name)
export MSH_PACKAGE_NAME="com.mongodb.hadoop.hive.MongoStorageHandler"

# URL of MongoDB collection to use in testing --- CHANGE ME ---
export MONGO_TEST_URI="<uri to MONGO><ex. mongodb://localhost:17000/test.mongo_hive_test>"

# serde properties =>
#   specify a comma-delimited serde properties mapping for test data file
#   USE SINGLE QUOTES to wrap the value of 'mongo.columns.mapping'
export SERDE_PROPERTIES="'mongo.columns.mapping'='{\"id\":\"_id\"}'"

# run tests in "verbose" mode; 0 -> False, 1 -> True
export VERBOSE_TESTS=1

############################################################################
# path of "mongod" executable
export MONGOD="mongod"

# <dbpath> of mongod --- CHANGE ME ---
export DB_PATH="/usr/local/opt/mongodb/bin/"

# logpath of mongod
export LOG_PATH="$DB_PATH/mongodb.log"

# port of mongod --- CHANGE ME ---
export MONGOD_PORT=27017
export HIVE_HOME=/Users/jlee/hadoop-binaries/hive-0.12.0

# add python Thrift libraries for using HiveClient --- CHANGE ME ---
export PYTHONPATH=$PYTHONPATH:$HIVE_HOME/lib/py
export PATH=$PATH:$HIVE_HOME/bin

# make sure that hadoop daemons are running
check_hadoop=$(jps | grep -iE 'namenode|datanode|nodemanager|resourcemanager' | wc -l)
if [ "$check_hadoop" -lt "4" ]; then
    echo "Make sure you've started the Hadoop daemons: namenode, datanode, nodemanager, and resourcemanager"
    #exit 1
else
    echo "Hadoop daemons already running"
fi

# also, make sure "mongod" is running. If not, start it
check_mongod=$(ps aux | grep "mongod" | wc -l)
if [ "$check_mongod" -lt "2" ]; then
    $MONGOD --dbpath $DB_PATH --port $MONGOD_PORT --fork --logpath $LOG_PATH
    echo "Starting 'mongod'"
else
    echo "'mongod' already running"
fi

# run the python unit tests
python mongo_hive_tests.py

