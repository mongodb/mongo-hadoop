documents =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.replace_test'
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray,i:int', 'id');

increment_number =
    FOREACH documents
    GENERATE com.mongodb.hadoop.pig.udf.ToObjectId(id) AS id,
             i + 1 AS i;

STORE increment_number
    INTO 'mongodb://localhost:27017/mongo_hadoop.replace_test'
    USING com.mongodb.hadoop.pig.MongoUpdateStorage(
        '{_id:"\$id"}',  -- query
        '{i:"\$i"}',  -- replacement
        'id:bytearray,i:int',  -- schema
        '',  -- toIgnore (none)
        '{replace:true}'  -- update options
    );
