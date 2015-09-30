-- Load data from BSON.
persons_info =
    LOAD '@PROJECT_HOME@/pig/src/test/resources/dump/test/persons_info.bson'
    USING com.mongodb.hadoop.pig.BSONLoader(
      'id', 'first: chararray, last: chararray, age: double')
    AS (first: chararray, last: chararray, age: double);

-- Dump into mongo, ensure index on first name.
STORE persons_info
    INTO 'mongodb://localhost:27017/mongo_hadoop.ensure_indexes'
    USING com.mongodb.hadoop.pig.MongoStorage(
        '{first: 1}, {}'
    );