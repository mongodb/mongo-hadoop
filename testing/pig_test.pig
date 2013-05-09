REGISTER /tmp/piggybank-0.3-amzn.jar
REGISTER  /Users/mike/Downloads/mongo-2.10.1.jar;
REGISTER  /Users/mike/piggybank.jar;
REGISTER ../core/target/mongo-hadoop-core_0.20.205.0-1.1.0-SNAPSHOT.jar
REGISTER ../pig/target/mongo-hadoop-pig_0.20.205.0-1.1.0-SNAPSHOT.jar

raw = LOAD 'file:///Users/mike/dump/mongo_hadoop/yield_historical.in.bson' using com.mongodb.hadoop.pig.BSONLoader; 
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE FORMAT org.apache.pig.piggybank.evaluation.string.FORMAT();
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();

date_tenyear = foreach raw generate UnixToISO($0#'_id'), $0#'bc10Year';
parsed_year = foreach date_tenyear generate 
    FLATTEN(EXTRACT($0, '(\\d{4})')) AS year, (double)$1 as bc;

--DUMP parsed_year;
by_year = GROUP parsed_year BY (chararray)year;
--just_years = FOREACH by_year GENERATE group;
year_10yearavg = FOREACH by_year GENERATE group, AVG(parsed_year.bc);
dump year_10yearavg;
