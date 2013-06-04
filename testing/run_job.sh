#!/bin/sh


#Set your HADOOP_HOME directory here.
export HADOOP_HOME="/Users/mike/hadoop/hadoop-2.0.0-cdh4.3.0" 

declare -a job_args

job_args=("jar" "examples/treasury_yield/target/treasury-example_cdh4.3.0-1.1.0.jar")
job_args=(${job_args[@]} "com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig")
job_args=(${job_args[@]} "-D" "mongo.job.verbose=true")

# INPUT SOURCE -
# To use a mongo collection as input:
job_args=(${job_args[@]} "-D" "mongo.job.input.format=com.mongodb.hadoop.MongoInputFormat")
job_args=(${job_args[@]} "-D" "mongo.input.uri=mongodb://localhost:27017/demo.yield_historical.in")

# To use a BSON file as input, use these two lines instead:
#job_args=(${job_args[@]} "-D" "mongo.job.input.format=com.mongodb.hadoop.BSONFileInputFormat")
#job_args=(${job_args[@]} "-D" 'mapred.input.dir=file:///Users/mike/dump/mongo_hadoop/yield_historical.in.bson')

#Set the classes used for Mapper/Reducer
job_args=(${job_args[@]} "-D" "mongo.job.mapper=com.mongodb.hadoop.examples.treasury.TreasuryYieldMapper")
job_args=(${job_args[@]} "-D" "mongo.job.reducer=com.mongodb.hadoop.examples.treasury.TreasuryYieldReducer")

#Set the values used for output keys + values.
job_args=(${job_args[@]} "-D" "mongo.job.output.key=org.apache.hadoop.io.IntWritable")
job_args=(${job_args[@]} "-D" "mongo.job.output.value=org.apache.hadoop.io.DoubleWritable")

job_args=(${job_args[@]} "-D" "mongo.job.partitioner=")
job_args=(${job_args[@]} "-D" "mongo.job.sort_comparator=")
job_args=(${job_args[@]} "-D" "mongo.job.background=false")

# OUTPUT
# To send the output to a mongo collection:
job_args=(${job_args[@]} "-D" "mongo.output.uri=mongodb://localhost:27017/demo.yield_historical.out")
job_args=(${job_args[@]} "-D" "mongo.job.output.format=com.mongodb.hadoop.MongoOutputFormat")

# To write the output to a .BSON file instead, use these two lines:
#job_args=(${job_args[@]} "-D" "mapred.output.file=file:///tmp/yield_historical.out")
#job_args=(${job_args[@]} "-D" "mongo.job.output.format=com.mongodb.hadoop.BSONFileOutputFormat")

$HADOOP_HOME/bin/hadoop "${job_args[@]}" "$1"

