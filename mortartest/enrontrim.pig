REGISTER ../pig/target/mongo-hadoop-pig_0.20.2-hawk-1.0.0.jar;
REGISTER ../target/mongo-hadoop_0.20.2-hawk-1.0.0.jar;
REGISTER ../core/target/mongo-hadoop-core_0.20.2-hawk-1.0.0.jar;
REGISTER ../examples/pigtutorial/lib/mongo-java-driver.jar;
REGISTER 'foo.py' USING streaming_python;

raw = LOAD 'mongodb://localhost/demo.enronmedium' USING com.mongodb.hadoop.pig.MongoLoader('body:chararray, headers:tuple(From, Subject, To )');

flat = FOREACH raw GENERATE headers.From as from, headers.To as to, body;

clean = FILTER flat BY from is not null and to is not null;

splitT =  FOREACH clean GENERATE deserialize_receivers(from, to, body);

flatSplit = FOREACH splitT GENERATE flatten(emails);

rank = FOREACH flatSplit GENERATE FROM as from, TO as to, rate_email(BODY) as score;

grouped = GROUP rank BY (from, to);

counted = FOREACH grouped GENERATE group.from as from, group.to as to, SUM(rank.score) as score;

internal = FILTER counted BY is_enron(from) == 1 and is_enron(to) == 1;

ordered = ORDER internal BY score desc;

-- ordered = ORDER counted BY From;


-- STORE splitT INTO 'output' USING PigStorage('\t');
STORE ordered INTO 'mongodb://localhost/demo.enrontrim' USING com.mongodb.hadoop.pig.MongoStorage('update [time, servername, hostname]', '{time : 1, servername : 1, hostname : 1}, {unique:false, dropDups: false}');



--REGISTER 'foo.py' USING streaming_python;


raw = LOAD 'mongodb://tmillar:mortartest@ds033097.mongolab.com:33097/mortardb.enronmedium' USING com.mongodb.hadoop.pig.MongoLoader('body:chararray, headers:tuple(From, Subject, To, Date)');

flat = FOREACH raw GENERATE headers.From as from, headers.To as to, body as body:chararray, headers.Date as date;

clean = FILTER flat BY from is not null and to is not null;

--splitT =  FOREACH clean GENERATE deserialize_receivers(from, to, body);

--flatSplit = FOREACH splitT GENERATE flatten(emails);

-- rank = FOREACH flatSplit GENERATE FROM as from, TO as to, rate_email(BODY) as score;

rank = FOREACH clean GENERATE from, to, rate_email(body) as score, time_bucket(date) as date;

grouped = GROUP rank BY (from, date);

counted = FOREACH grouped GENERATE group.from as from, group.date as date, SUM(rank.score) as score;

--internal = FILTER counted BY is_enron(from) == 1 and is_enron(to) == 1;

ordered = ORDER counted BY score;

STORE ordered INTO 'mongodb://tmillar:mortartest@ds033097.mongolab.com:33097/mortardb.enrontrim' USING com.mongodb.hadoop.pig.MongoStorage('update [time, servername, hostname]', '{time : 1, servername : 1, hostname : 1}, {unique:false, dropDups: false}');