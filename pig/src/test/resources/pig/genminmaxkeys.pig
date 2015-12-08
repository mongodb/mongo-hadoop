data =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.udftest.input'
    USING com.mongodb.hadoop.pig.MongoLoader;

create_min_max_keys =
    FOREACH data
    GENERATE com.mongodb.hadoop.pig.udf.GenMaxKey() AS newMax,
             com.mongodb.hadoop.pig.udf.GenMinKey() AS newMin;

STORE create_min_max_keys
    INTO 'mongodb://localhost:27017/mongo_hadoop.udftest.output'
    USING com.mongodb.hadoop.pig.MongoInsertStorage;
