-- no schema provided
data = LOAD 'mongodb://localhost:27017/mongo_hadoop.pig.schemaless'
       USING com.mongodb.hadoop.pig.MongoLoader;

-- no schema or id provided
STORE data INTO 'mongodb://localhost:27017/mongo_hadoop.pig.schemaless.out'
           USING com.mongodb.hadoop.pig.MongoInsertStorage;