data =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.pigtests'
    USING com.mongodb.hadoop.pig.MongoLoader('today:datetime');

STORE data
    INTO 'mongodb://localhost:27017/mongo_hadoop.datetest'
    USING com.mongodb.hadoop.pig.MongoInsertStorage;
