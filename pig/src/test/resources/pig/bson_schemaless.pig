REGISTER @PROJECT_HOME@/core/build/libs/mongo-hadoop-core-@PROJECT_VERSION@.jar
REGISTER @PROJECT_HOME@/pig/build/libs/mongo-hadoop-pig-@PROJECT_VERSION@.jar

-- Load data from BSON, providing no schema.
persons_info =
    LOAD '@PROJECT_HOME@/pig/src/test/resources/dump/test/persons_info.bson'
    USING com.mongodb.hadoop.pig.BSONLoader;

-- Insert into MongoDB.
STORE persons_info
    INTO 'mongodb://localhost:27017/mongo_hadoop.bson_schemaless'
    USING com.mongodb.hadoop.pig.MongoInsertStorage;

-- Get the results back from mongo.
results = LOAD 'mongodb://localhost:27017/mongo_hadoop.bson_schemaless'
          USING com.mongodb.hadoop.pig.MongoLoader('first, last, age');
