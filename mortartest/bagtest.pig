/**
 * Using the Excite search log data and fake users data, determine
 * which age group of users (e.g. 20-29, 30-39, etc) are the most prolific
 * searchers, and which age group uses the biggest words. :-)
 */
REGISTER pigtutorial.jar;
REGISTER ../pig/target/mongo-hadoop-pig_0.20.2-hawk-1.0.0.jar;
REGISTER ../target/mongo-hadoop_0.20.2-hawk-1.0.0.jar;
REGISTER ../core/target/mongo-hadoop-core_0.20.2-hawk-1.0.0.jar;
REGISTER mongo-java-driver.jar;
 

grouped = LOAD 'mongodb://tmillar:mortartest@ds033097.mongolab.com:33097/mortardb.test' USING com.mongodb.hadoop.pig.MongoLoader('group', 'joined') AS (group:bytearray, joined:bag{t:(clean_searches::user_id, clean_searches::timestamp, clean_searches::query, users_age_buckets::age_bucket)});

-- Calculate metrics on each age bucket
age_buckets = FOREACH grouped 
             GENERATE group as age_bucket,
                      COUNT(joined) as num_searches,
                      joined as avg_word_length;


-- Store in MongoDB
STORE grouped INTO 'mongodb://tmillar:mortartest@ds033097.mongolab.com:33097/mortardb.second' USING com.mongodb.hadoop.pig.MongoStorage('update [time, servername, hostname]', '{time : 1, servername : 1, hostname : 1}, {unique:true, dropDups: true}');



