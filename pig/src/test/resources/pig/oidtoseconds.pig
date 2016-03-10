data =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.udftest.input'
    USING com.mongodb.hadoop.pig.MongoLoader;

calc_seconds =
    FOREACH data
    GENERATE com.mongodb.hadoop.pig.udf.ToObjectId($0#'_id') AS id,
             com.mongodb.hadoop.pig.udf.ObjectIdToSeconds($0#'_id') AS seconds,
             -- Make sure we can nest UDFs.
             com.mongodb.hadoop.pig.udf.ObjectIdToSeconds(
                 com.mongodb.hadoop.pig.udf.ToObjectId($0#'_id')) AS seconds2;

STORE calc_seconds
    INTO 'mongodb://localhost:27017/mongo_hadoop.udftest.output'
    USING com.mongodb.hadoop.pig.MongoInsertStorage('id');
