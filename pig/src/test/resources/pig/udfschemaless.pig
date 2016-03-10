data =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.udftest.input'
    USING com.mongodb.hadoop.pig.MongoLoader;

create_objids =
    FOREACH data
    GENERATE com.mongodb.hadoop.pig.udf.ToObjectId($0#'_id');

STORE create_objids
    INTO 'mongodb://localhost:27017/mongo_hadoop.udftest.output'
    USING com.mongodb.hadoop.pig.MongoInsertStorage;
