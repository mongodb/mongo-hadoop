REGISTER ../pig/target/mongo-hadoop-pig_1.0.2-1.0.0.jar;
REGISTER ../target/mongo-hadoop_1.0.2-1.0.0.jar;
REGISTER ../core/target/mongo-hadoop-core_1.0.2-1.0.0.jar;
REGISTER mongo-java-driver.jar;

raw = LOAD 'mongodb://localhost/demo.pig' USING com.mongodb.hadoop.pig.MongoLoader('score', 'count', 'mean') AS (score:long, count:long, mean:double);


mult = foreach raw generate score*count as x:long;

STORE mult INTO 'mongodb://localhost/demo.test' USING com.mongodb.hadoop.pig.MongoStorage('update [time, servername, hostname]', '{time : 1, servername : 1, hostname : 1}, {unique:false, dropDups: false}');