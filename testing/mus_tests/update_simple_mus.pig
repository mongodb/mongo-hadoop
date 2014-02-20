-- update jar positions to point to their correct positions on your system
REGISTER /usr/local/Cellar/hadoop/1.1.2/libexec/lib/mongo-2.10.1.jar
REGISTER /usr/local/Cellar/hadoop/1.1.2/libexec/lib/mongo-hadoop-core_1.1.2-1.1.0.jar
REGISTER /usr/local/Cellar/hadoop/1.1.2/libexec/lib/mongo-hadoop-pig-1.1.0.jar
REGISTER ./helpers.jar

persons_info = LOAD 'dump/test/persons_info.bson' 
          USING com.mongodb.hadoop.pig.BSONLoader;

to_store = FOREACH persons_info
       GENERATE $0#'first' as first, 
            $0#'last' as last,
            helpers.TOBAG($0#'cars') as cars;

dump to_store;

STORE to_store INTO 'mongodb://localhost:27017/test.update_mus'
               USING com.mongodb.hadoop.pig.MongoUpdateStorage(
             '{first:"\$first", last:"\$last"}',
             '{\$pushAll:{cars:"\$cars"}}',
             'first:chararray, last:chararray, cars:{b:(t:chararray)}', 't');

