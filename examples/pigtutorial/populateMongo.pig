REGISTER target/mongo-hadoop.jar;
REGISTER pig/target/mongo-hadoop-pig.jar
REGISTER core/target/mongo-hadoop-core.jar
REGISTER examples/pigtutorial/lib/mongo-java-driver.jar;

raw = LOAD 'examples/pigtutorial/resources/excite-small.log' USING PigStorage('\t') AS (user, time, query);

STORE raw INTO 'mongodb://localhost/demo.excitelog' USING com.mongodb.hadoop.pig.MongoStorage('update [time, servername, hostname]', '{time : 1, servername : 1, hostname : 1}, {unique:false, dropDups: false}');