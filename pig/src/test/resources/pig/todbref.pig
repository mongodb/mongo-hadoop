data =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.udftest.input'
    USING com.mongodb.hadoop.pig.MongoLoader('dbref:[]');

create_dbref =
    FOREACH data
    GENERATE com.mongodb.hadoop.pig.udf.ToDBRef($0) AS dbref;

STORE create_dbref
    INTO 'mongodb://localhost:27017/mongo_hadoop.udftest.output'
    USING com.mongodb.hadoop.pig.MongoInsertStorage;
