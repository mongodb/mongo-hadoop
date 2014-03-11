#!/bin/sh

###########Environment settings#############
# Modify these to match your setup.
#export HADOOP_HOME="/Users/mike/hadoop/hadoop-1.1.2" 
INPUT_URI="mongodb://localhost:27017/mongo_hadoop.yield_historical.in"
OUTPUT_URI="mongodb://localhost:27017/mongo_hadoop.yield_historical.out"
JARNAME="treasury_yield-1.2.1-SNAPSHOT-hadoop_2.2.jar"

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

#Set the filename of the jar to match the jar you built depending
#on your hadoop version.
declare -a job_args
job_args=("jar" "$HERE/build/libs/$JARNAME")
job_args=(${job_args[@]} "com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig")
job_args=(${job_args[@]} "-D" "mongo.job.verbose=true")

# INPUT SOURCE -
# To use a mongo collection as input:
job_args=(${job_args[@]} "-D" "mongo.job.input.format=com.mongodb.hadoop.MongoInputFormat")
job_args=(${job_args[@]} "-D" "mongo.input.uri=$INPUT_URI")

#Split settings
job_args=(${job_args[@]} "-D" "mongo.input.split_size=8")

# To use a BSON file as input, use these two lines instead:
#job_args=(${job_args[@]} "-D" "mongo.job.input.format=com.mongodb.hadoop.BSONFileInputFormat")
#job_args=(${job_args[@]} "-D" 'mapred.input.dir=file:///Users/mike/dump/mongo_hadoop/yield_historical.in.bson')

#Set the classes used for Mapper/Reducer
job_args=(${job_args[@]} "-D" "mongo.job.mapper=com.mongodb.hadoop.examples.treasury.TreasuryYieldMapper")
job_args=(${job_args[@]} "-D" "mongo.job.reducer=com.mongodb.hadoop.examples.treasury.TreasuryYieldReducer")

#Set the values used for output keys + values.
job_args=(${job_args[@]} "-D" "mongo.job.output.key=org.apache.hadoop.io.IntWritable")
job_args=(${job_args[@]} "-D" "mongo.job.output.value=com.mongodb.hadoop.io.BSONWritable")

job_args=(${job_args[@]} "-D" "mongo.job.mapper.output.key=org.apache.hadoop.io.IntWritable")
job_args=(${job_args[@]} "-D" "mongo.job.mapper.output.value=org.apache.hadoop.io.DoubleWritable")

# OUTPUT
# To send the output to a mongo collection:
job_args=(${job_args[@]} "-D" "mongo.output.uri=$OUTPUT_URI")
job_args=(${job_args[@]} "-D" "mongo.job.output.format=com.mongodb.hadoop.MongoOutputFormat")

# Alternatively, to write the output to a .BSON file use these two lines instead:
#job_args=(${job_args[@]} "-D" "mapred.output.dir=file:///tmp/yield_historical_out.bson")
#job_args=(${job_args[@]} "-D" "mongo.job.output.format=com.mongodb.hadoop.BSONFileOutputFormat")

echo "${job_args[@]}" "$1" 

$HADOOP_HOME/bin/hadoop "${job_args[@]}" "$1" 2>&1 | tee hadoop.out

