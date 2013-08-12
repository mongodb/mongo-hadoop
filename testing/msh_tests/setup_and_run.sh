# set some parameters to be used in the 'test' script
export TEST_DATA_FILE="/Users/danielalabi/data.txt"
export HIVE_TEST_SCHEMA="(id int, name string, age int)"
export HIVE_TEST_FILE_TYPE="textfile"
export MSH_PATH="/Users/danielalabi/hive_libraries/msh.jar"
export MSH_PACKAGE_NAME="com.mongodb.hadoop.hive.MongoStorageHandler"
export MONGO_TEST_URI="mongodb://localhost:27017/test.mongo_hive_test"
export MONGOD="mongod"
export MONGOD_PATH="/Users/danielalabi/mongodb"
export MONGOD_PORT=27017
# serde properties =>
#   specify a comma-delimited serde properties mapping
export SERDE_PROPERTIES="'mongo.columns.mapping'='_id, name, age'"
export VERBOSE_TESTS=true
export PYTHONPATH=$PYTHONPATH:/usr/local/Cellar/hive/0.10.0/libexec/lib/py/

# before starting the tests, make sure "mongod" is running
check_mongod=$(ps aux | grep "mongod" | wc -l)
if [ "$check_mongod" -lt "2" ]
    then $MONGOD --dbpath $MONGOD_PATH --port $MONGOD_PORT --fork --syslog 
fi

# run the python tests
python run_mongo_hive_tests.py
