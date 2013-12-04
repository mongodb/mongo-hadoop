-- Update these jar locations to point to the correct location on your machine
REGISTER /tmp/piggybank.jar
REGISTER  /Users/mike/Downloads/mongo-java-driver-2.11.2.jar;
REGISTER /Users/mike/projects/mongo-hadoop/pig/target/mongo-hadoop-pig_1.1.2-1.1.0.jar
REGISTER /Users/mike/projects/mongo-hadoop/core/target/mongo-hadoop-core_1.1.2-1.1.0.jar

raw = LOAD 'mongodb://localhost:27017/demo.yield_historical.in' using com.mongodb.hadoop.pig.MongoLoader;
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE RegexExtract org.apache.pig.piggybank.evaluation.string.RegexExtract();

date_tenyear = foreach raw generate UnixToISO($0#'_id'), $0#'bc10Year';
parsed_year = foreach date_tenyear generate RegexExtract($0, '(\\d{4})', 0) AS year, (double)$1 as bc;

by_year = GROUP parsed_year BY (chararray)year;
year_10yearavg = FOREACH by_year GENERATE group, AVG(parsed_year.bc);
dump year_10yearavg;
