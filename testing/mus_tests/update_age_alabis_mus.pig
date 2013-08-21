-- update jar locations to point to their correct positions on your system
REGISTER /usr/local/Cellar/hadoop/1.1.2/libexec/lib/mongo-2.10.1.jar
REGISTER /usr/local/Cellar/hadoop/1.1.2/libexec/lib/mongo-hadoop-core_1.1.2-1.1.0.jar
REGISTER /usr/local/Cellar/hadoop/1.1.2/libexec/lib/mongo-hadoop-pig_1.1.2-1.1.0.jar

persons_info = LOAD 'dump/test/persons_info.bson' 
          USING com.mongodb.hadoop.pig.BSONLoader;

to_store = FOREACH persons_info
           GENERATE $0#'first' as first,
                $0#'last' as last,
                $0#'age' as age;

dump to_store;

STORE to_store INTO 'mongodb://localhost:27017/test.update_mus'
               USING com.mongodb.hadoop.pig.MongoUpdateStorage(
             '{last:"Alabi"}',
             '{\$set:{age:30}}', 
             'first:chararray, last:chararray, age:int', '',
             '{multi : true}');

