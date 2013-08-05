-- Update these jar locations to point to the correct location on your machine
REGISTER /tmp/piggybank-0.3-amzn.jar
REGISTER /tmp/piggybank.jar
REGISTER  /Users/mike/Downloads/mongo-java-driver-2.11.2.jar;
REGISTER /Users/mike/projects/mongo-hadoop/pig/target/mongo-hadoop-pig_1.1.2-1.1.0.jar
REGISTER /Users/mike/projects/mongo-hadoop/core/target/mongo-hadoop-core_1.1.2-1.1.0.jar

raw = LOAD 'mongodb://localhost:27017/demo.yield_historical.in' using com.mongodb.hadoop.pig.MongoLoader; 
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE FORMAT org.apache.pig.piggybank.evaluation.string.FORMAT();
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();

date_tenyear = foreach raw generate UnixToISO($0#'_id'), $0#'bc10Year';
parsed_year = foreach date_tenyear generate FLATTEN(EXTRACT($0, '(\\d{4})')) AS year, (double)$1 as bc;

by_year = GROUP parsed_year BY (chararray)year;
year_10yearavg = FOREACH by_year GENERATE group, AVG(parsed_year.bc);
dump year_10yearavg;
