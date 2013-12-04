-- Update these jar locations to point to the correct location on your machine
REGISTER  /Users/mike/Downloads/mongo-2.10.1.jar;
REGISTER  /Users/mike/piggybank.jar;
REGISTER ../core/target/mongo-hadoop-core_0.20.205.0-1.1.0.jar
REGISTER ../pig/target/mongo-hadoop-pig_0.20.205.0-1.1.0.jar

raw = LOAD 'file:///Users/mike/dump/mongo_hadoop/yield_historical.in.bson' using com.mongodb.hadoop.pig.BSONLoader; 
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE RegexExtract org.apache.pig.piggybank.evaluation.string.RegexExtract();

date_tenyear = foreach raw generate UnixToISO($0#'_id'), $0#'bc10Year';
parsed_year = foreach date_tenyear generate RegexExtract($0, '(\\d{4})', 0) AS year, (double)$1 as bc;

by_year = GROUP parsed_year BY (chararray)year;
year_10yearavg = FOREACH by_year GENERATE group, AVG(parsed_year.bc);
dump year_10yearavg;
