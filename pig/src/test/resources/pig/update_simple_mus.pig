REGISTER @PROJECT_HOME@/core/build/libs/mongo-hadoop-core-@PROJECT_VERSION@.jar
REGISTER @PROJECT_HOME@/pig/build/libs/mongo-hadoop-pig-@PROJECT_VERSION@.jar
REGISTER @PROJECT_HOME@/pig/build/libs/mongo-hadoop-pig-@PROJECT_VERSION@-tests.jar

-- Load data from BSON.
persons_info =
    LOAD '@PROJECT_HOME@/pig/src/test/resources/dump/test/persons_info.bson'
    USING com.mongodb.hadoop.pig.BSONLoader;

-- Parse data from BSON into tuples so we can address fields when doing an
-- update. Explicitly define the schema for the 'cars' bag so we can write it
-- out later with MongoInsertStorage.
to_store =
    FOREACH persons_info
    GENERATE
        $0#'first' as first,
        $0#'last' as last,
        helpers.TOBAG($0#'cars') as cars: bag{t: tuple(car: chararray)};

-- Insert into MongoDB.
STORE to_store
    INTO 'mongodb://localhost:27017/mongo_hadoop.update_mus'
    USING com.mongodb.hadoop.pig.MongoInsertStorage;

-- Perform the update (everyone gets 2x their cars).
STORE to_store
    INTO 'mongodb://localhost:27017/mongo_hadoop.update_mus'
    USING com.mongodb.hadoop.pig.MongoUpdateStorage(
        '{first:"\$first", last:"\$last"}',
        '{\$pushAll:{cars:"\$cars"}}');

-- Get the results back from mongo.
results = LOAD 'mongodb://localhost:27017/mongo_hadoop.update_mus'
          USING com.mongodb.hadoop.pig.MongoLoader('first, last, cars');
