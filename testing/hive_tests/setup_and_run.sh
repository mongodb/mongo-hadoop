# !/usr/bin/env bash

## sets some parameters to be used in the 'test' script. Starts required
## daemons (mongod, hive, hadoop). Runs tests in standalone mode (single-node cluster).
##
## To run these unit tests, you must have the following installed:
## * hive (+ accompanying Python HiveClient and Thrift libraries)
## * hadoop
## * MongoDB 
## * mongo-hadoop-hive-*.jar

# location of data file used in testing
export TEST_DATA_FILE="./test_data.txt"
# directory containing BSON file to be used for testing -- CHANGE ME --
export TEST_BSON_FILE_PATH="/Users/danielalabi/mongo-hadoop-sweet/testing/hive_tests/bson_test/"
# schema of the $TEST_DATA_FILE -- should be in hive schema format
export HIVE_TEST_SCHEMA="(id int, name string, age int)"
# Type of data file. For example, sequencefile, rcfile, textfile
export HIVE_TEST_FILE_TYPE="textfile"
# path (absolute or relative) where MongoStorageHandler resides
export MSH_PATH="../../hive/target/mongo-hadoop-hive-1.1.0.jar"
# Class name of MongoStorageHandler (prefixed by package name)
export MSH_PACKAGE_NAME="com.mongodb.hadoop.hive.MongoStorageHandler"
# URL of MongoDB collection to use in testing
export MONGO_TEST_URI="mongodb://localhost:27017/test.mongo_hive_test"
# serde properties =>
#   specify a comma-delimited serde properties mapping for test data file
export SERDE_PROPERTIES="'mongo.columns.mapping'='_id, name, age'"
# run tests in "verbose" mode; 0 -> False, 1 -> True
export VERBOSE_TESTS=1

# path of "mongod" executable
export MONGOD="mongod"
# <dbpath> of mongod --- CHANGE ME ---
export DB_PATH="/Users/danielalabi/mongodb"
# logpath of mongod
export LOG_PATH="$DB_PATH/mongodb.log"
# port of mongod
export MONGOD_PORT=27017
# add python Thrift libraries for using HiveClient
export PYTHONPATH=$PYTHONPATH:/usr/local/Cellar/hive/0.10.0/libexec/lib/py/

# make sure that hadoop daemons are running
check_hadoop=$(jps | grep -iE 'namenode|datanode|tasktracker|jobtracker' | wc -l)
if [ "$check_hadoop" -lt "4" ]; then
    echo "Make sure you've started the Hadoop daemons: namenode, datanode, tasktracker, and jobtracker"
    exit 1
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
python ./mongo_hive_tests.py

# clean-up: stop hadoop daemons and mongod, maybe?
