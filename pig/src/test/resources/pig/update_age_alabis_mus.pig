REGISTER @JAVA_DRIVER_JAR@
REGISTER @PROJECT_HOME@/core/build/libs/mongo-hadoop-core-@PROJECT_VERSION@.jar
REGISTER @PROJECT_HOME@/pig/build/libs/mongo-hadoop-pig-@PROJECT_VERSION@.jar

persons_info = LOAD '@PROJECT_HOME@/pig/build/dump/test/persons_info.bson' 
          USING com.mongodb.hadoop.pig.BSONLoader;

to_store = FOREACH persons_info
           GENERATE $0#'first' as first,
                $0#'last' as last,
                $0#'age' as age;

dump to_store;

STORE to_store INTO 'mongodb://localhost:27017/mongo_hadoop.update_mus'
               USING com.mongodb.hadoop.pig.MongoUpdateStorage(
             '{last:"Alabi"}',
             '{\$set:{age:30}}', 
             'first:chararray, last:chararray, age:int', '',
             '{multi : true}');

