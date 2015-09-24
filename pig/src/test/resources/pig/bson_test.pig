REGISTER @PROJECT_HOME@/core/build/libs/mongo-hadoop-core-@PROJECT_VERSION@.jar
REGISTER @PROJECT_HOME@/pig/build/libs/mongo-hadoop-pig-@PROJECT_VERSION@.jar

-- Load data from BSON.
persons_info =
    LOAD '@PROJECT_HOME@/pig/src/test/resources/dump/test/persons_info.bson'
    USING com.mongodb.hadoop.pig.BSONLoader;

-- Make sure the BSON doesn't already exist.
rmf file://@PIG_RESOURCES@/pig/test_output

STORE persons_info
    INTO 'file://@PIG_RESOURCES@/pig/test_output'
    USING com.mongodb.hadoop.pig.BSONStorage;

persons_read =
    LOAD 'file://@PIG_RESOURCES@/pig/test_output'
    USING com.mongodb.hadoop.pig.BSONLoader(
      'id', 'first: chararray, last: chararray, age: double')
    AS (first: chararray, last: chararray, age: double);

DUMP persons_read;
