REGISTER /tmp/piggybank-0.3-amzn.jar
REGISTER  /Users/mike/Downloads/mongo-2.10.1.jar;
REGISTER  /Users/mike/piggybank.jar;
REGISTER ../core/target/mongo-hadoop-core_0.20.205.0-1.1.0-SNAPSHOT.jar
REGISTER ../pig/target/mongo-hadoop-pig_0.20.205.0-1.1.0-SNAPSHOT.jar

raw = LOAD 'file:///Users/mike/dump/enron_mail/messages.bson' using com.mongodb.hadoop.pig.BSONLoader('','headers:[]') ; 
--raw_limited = LIMIT raw 10;
send_recip = FOREACH raw GENERATE $0#'From' as from, $0#'To' as to;
send_recip_filtered = FILTER send_recip BY to IS NOT NULL;
send_recip_grouped = GROUP send_recip_filtered BY (from, to);
send_recip_counted = FOREACH send_recip_grouped GENERATE group, COUNT($1) as count;
STORE send_recip_counted INTO 'file:///tmp/poop.bson' using com.mongodb.hadoop.pig.BSONStorage;
--DUMP send_recip_counted;
--raw_grouped = GROUP raw BY 
--STORE raw_limited INTO 'file:///tmp/poop.bson' using com.mongodb.hadoop.pig.BSONStorage;

--DUMP raw_limited;

--raw = LOAD 'file:///Users/mike/dump/mongo_hadoop/yield_historical.in.bson' using com.mongodb.hadoop.pig.BSONLoader; 
--DUMP raw;
/*
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE FORMAT org.apache.pig.piggybank.evaluation.string.FORMAT();
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();

date_tenyear = foreach raw generate UnixToISO($0#'_id'), $0#'bc10Year';
parsed_year = foreach date_tenyear generate 
    FLATTEN(EXTRACT($0, '(\\d{4})')) AS year, (double)$1 as bc;

--DUMP parsed_year;
by_year = GROUP parsed_year BY (chararray)year;
--just_years = FOREACH by_year GENERATE group;
year_10yearavg = FOREACH by_year GENERATE group, AVG(parsed_year.bc)  as bc10Year;

STORE year_10yearavg INTO 'file:///tmp/blah.bson' USING com.mongodb.hadoop.pig.BSONStorage('group'); --by_year, by_year);
*/
