REGISTER ../pig/target/mongo-hadoop-pig_0.20.2-hawk-1.0.0.jar;
REGISTER ../target/mongo-hadoop_0.20.2-hawk-1.0.0.jar;
REGISTER ../core/target/mongo-hadoop-core_0.20.2-hawk-1.0.0.jar;
REGISTER ../examples/pigtutorial/lib/mongo-java-driver.jar;

raw = LOAD 'mongodb://localhost/demo.enronfull' USING com.mongodb.hadoop.pig.MongoLoader('body:chararray, headers:tuple(From, Subject, To, Date )');

STORE raw INTO 'mongodb://mhc_data:rags2313Forage@ds031667-a1.mongolab.com:31667/mhc_data.enronmedium' USING com.mongodb.hadoop.pig.MongoStorage('update [time, servername, hostname]', '{time : 1, servername : 1, hostname : 1}, {unique:false, dropDups: false}');


mongorestore --host ds031667-a1.mongolab.com --port 31667 --username mhc_data --password rags2313Forage --db mhc_data --collection enron enron_mongo.tar.bz2
