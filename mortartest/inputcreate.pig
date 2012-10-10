
-- Based on the Pig tutorial ,modified for Mongo support tests
REGISTER pigtutorial.jar;
REGISTER ../pig/target/mongo-hadoop-pig_0.20.2-hawk-1.0.0.jar;
REGISTER ../target/mongo-hadoop_0.20.2-hawk-1.0.0.jar;
REGISTER ../core/target/mongo-hadoop-core_0.20.2-hawk-1.0.0.jar;
REGISTER mongo-java-driver.jar;

-- Use the PigStorage function to load the excite log file into the raw bag as an array of records.
-- Input: (user,time,query) 
raw = LOAD 'excite-small.log' USING PigStorage('\t') AS (user, time, query);

-- Use the PigStorage function to store the results. 
-- Output: (hour, n-gram, score, count, average_counts_among_all_hours)
STORE raw INTO 'mongodb://localhost/demo.input' USING com.mongodb.hadoop.pig.MongoStorage('update [time, servername, hostname]', '{time : 1, servername : 1, hostname : 1}, {unique:false, dropDups: false}');

