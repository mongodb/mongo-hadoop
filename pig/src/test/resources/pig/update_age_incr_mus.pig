REGISTER @JAVA_DRIVER_JAR@
REGISTER @PROJECT_HOME@/core/build/libs/mongo-hadoop-core-@PROJECT_VERSION@.jar
REGISTER @PROJECT_HOME@/pig/build/libs/mongo-hadoop-pig-@PROJECT_VERSION@.jar

persons_info = LOAD '@PROJECT_HOME@/pig/build/dump/test/persons_info.bson' 
          USING com.mongodb.hadoop.pig.BSONLoader;

to_store = FOREACH persons_info
       GENERATE $0#'first' as first, 
            $0#'last' as last;
dump to_store;

STORE to_store INTO 'mongodb://localhost:27017/test.update_mus'
               USING com.mongodb.hadoop.pig.MongoUpdateStorage(
             '{first:"\$first", last:"\$last"}',
             '{\$inc:{age:1}}', 
             'first:chararray, last:chararray');

