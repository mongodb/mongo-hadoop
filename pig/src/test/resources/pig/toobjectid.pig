data =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.udftest.input'
    USING com.mongodb.hadoop.pig.MongoLoader(
        'id:chararray,oidBytes:bytearray', 'id');

create_objids =
    FOREACH data
    GENERATE com.mongodb.hadoop.pig.udf.ToObjectId(id) AS id,
             com.mongodb.hadoop.pig.udf.ToObjectId(oidBytes) AS otherid;

STORE create_objids
    INTO 'mongodb://localhost:27017/mongo_hadoop.udftest.output'
    USING com.mongodb.hadoop.pig.MongoInsertStorage('id');
