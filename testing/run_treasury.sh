#!/bin/sh
export HADOOP_HOME="/Users/mike/hadoop/hadoop-1.0.4"
declare -a job_args
cd ..
job_args=("jar" "examples/treasury_yield/target/treasury-example_*.jar")
job_args=(${job_args[@]} "-D" "mongo.job.verbose=true")
job_args=(${job_args[@]} "-D" "mongo.job.background=false")
job_args=(${job_args[@]} "-D" "mongo.input.key=")
job_args=(${job_args[@]} "-D" "mongo.input.uri=mongodb://127.0.0.1:4000,127.0.0.1:4001/mongo_hadoop.yield_historical.in?readPreference=secondary&replicaSet=replset0")
job_args=(${job_args[@]} "-D" "mongo.output.uri=mongodb://127.0.0.1:4000/mongo_hadoop.yield_historical.out")
job_args=(${job_args[@]} "-D" "mongo.input.query=")
job_args=(${job_args[@]} "-D" "mongo.job.mapper=com.mongodb.hadoop.examples.treasury.TreasuryYieldMapper")
job_args=(${job_args[@]} "-D" "mongo.job.reducer=com.mongodb.hadoop.examples.treasury.TreasuryYieldReducer")
job_args=(${job_args[@]} "-D" "mongo.job.input.format=com.mongodb.hadoop.MongoInputFormat")
job_args=(${job_args[@]} "-D" "mongo.job.output.format=com.mongodb.hadoop.MongoOutputFormat")
job_args=(${job_args[@]} "-D" "mongo.job.output.key=org.apache.hadoop.io.IntWritable")
job_args=(${job_args[@]} "-D" "mongo.job.output.value=org.apache.hadoop.io.DoubleWritable")
job_args=(${job_args[@]} "-D" "mongo.job.mapper.output.key=org.apache.hadoop.io.IntWritable")
job_args=(${job_args[@]} "-D" "mongo.job.mapper.output.value=org.apache.hadoop.io.DoubleWritable")
job_args=(${job_args[@]} "-D" "mongo.job.combiner=com.mongodb.hadoop.examples.treasury.TreasuryYieldReducer")
job_args=(${job_args[@]} "-D" "mongo.job.partitioner=")
job_args=(${job_args[@]} "-D" "mongo.job.sort_comparator=")

#echo "${job_args[@]}"
$HADOOP_HOME/bin/hadoop "${job_args[@]}" "$1"
