REGISTER @PROJECT_HOME@/core/build/libs/mongo-hadoop-core-@PROJECT_VERSION@.jar
REGISTER @PROJECT_HOME@/pig/build/libs/mongo-hadoop-pig-@PROJECT_VERSION@.jar

uuids =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.uuid_test'
    USING com.mongodb.hadoop.pig.MongoLoader('uuid');

STORE uuids INTO 'test_results';
