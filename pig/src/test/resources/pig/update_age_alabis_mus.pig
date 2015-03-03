REGISTER @PROJECT_HOME@/core/build/libs/mongo-hadoop-core-@PROJECT_VERSION@.jar
REGISTER @PROJECT_HOME@/pig/build/libs/mongo-hadoop-pig-@PROJECT_VERSION@.jar

-- Load data from BSON.
persons_info =
    LOAD '@PROJECT_HOME@/pig/src/test/resources/dump/test/persons_info.bson'
    USING com.mongodb.hadoop.pig.BSONLoader(
      'id', 'first: chararray, last: chararray, age: double')
    AS (first: chararray, last: chararray, age: double);

-- Insert into MongoDB.
STORE persons_info
    INTO 'mongodb://localhost:27017/mongo_hadoop.update_mus'
    USING com.mongodb.hadoop.pig.MongoInsertStorage;

-- Perform the update (everyone gets a little older).
STORE persons_info INTO 'mongodb://localhost:27017/mongo_hadoop.update_mus'
               USING com.mongodb.hadoop.pig.MongoUpdateStorage(
             '{}',
             '{\$inc:{age:1}}',
             'first, last, age', '',
             '{multi : true}');

-- Get the results back from mongo.
results =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.update_mus'
    USING com.mongodb.hadoop.pig.MongoLoader('first, last, age');
