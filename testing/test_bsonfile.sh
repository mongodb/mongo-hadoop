#!/bin/sh
declare -a job_args
cd ..
job_args=("jar" "examples/enron/target/enron-example*.jar")
job_args=(${job_args[@]} "-D" "mongo.job.verbose=true")
job_args=(${job_args[@]} "-D" "mongo.job.background=false")
#job_args=(${job_args[@]} "-D" "mongo.input.key=")
#job_args=(${job_args[@]} "-D" "mapred.input.dir=file:///Users/mike/dump/test/fbs.bson")
job_args=(${job_args[@]} "-D" "mapred.input.dir=file:///Users/mike/dump/enron_mail/messages2.bson")
job_args=(${job_args[@]} "-D" "mapred.map.tasks=8")
#job_args=(${job_args[@]} "-D" "mapred.input.dir=file:///Users/mike/dump/mongo_hadoop/yield_historical.in.bson")

job_args=(${job_args[@]} "-D" "mongo.output.uri=mongodb://127.0.0.1:27017/test.mrtestoutput")
#job_args=(${job_args[@]} "-D" "mongo.input.query=")
job_args=(${job_args[@]} "-D" "mongo.job.mapper=com.mongodb.hadoop.examples.enron.EnronMailMapper")
job_args=(${job_args[@]} "-D" "mongo.job.reducer=com.mongodb.hadoop.examples.enron.EnronMailReducer")
job_args=(${job_args[@]} "-D" "mongo.job.input.format=com.mongodb.hadoop.BSONFileInputFormat")
job_args=(${job_args[@]} "-D" "mongo.job.output.format=com.mongodb.hadoop.MongoOutputFormat")
job_args=(${job_args[@]} "-D" "mongo.job.output.key=com.mongodb.hadoop.io.BSONWritable")
job_args=(${job_args[@]} "-D" "mongo.job.output.value=org.apache.hadoop.io.IntWritable")
job_args=(${job_args[@]} "-D" "mongo.job.mapper.output.key=com.mongodb.hadoop.io.BSONWritable")
job_args=(${job_args[@]} "-D" "mongo.job.mapper.output.value=org.apache.hadoop.io.IntWritable")
#job_args=(${job_args[@]} "-D" "mongo.job.combiner=com.mongodb.hadoop.examples.enron.Enro")
#job_args=(${job_args[@]} "-D" "mongo.job.partitioner=")
#job_args=(${job_args[@]} "-D" "mongo.job.sort_comparator=")

echo "${job_args[@]}"
$HADOOP_HOME/bin/hadoop "${job_args[@]}" "$1"
