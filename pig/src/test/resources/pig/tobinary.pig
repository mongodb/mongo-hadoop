data =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.udftest.input'
    USING com.mongodb.hadoop.pig.MongoLoader('binary:bytearray');

create_bson_binary =
    FOREACH data
    GENERATE com.mongodb.hadoop.pig.udf.ToBinary(binary) AS binary;

STORE create_bson_binary
    INTO 'mongodb://localhost:27017/mongo_hadoop.udftest.output'
    USING com.mongodb.hadoop.pig.MongoInsertStorage;
