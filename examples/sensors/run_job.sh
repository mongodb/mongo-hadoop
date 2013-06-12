#!/bin/sh


mongo demo --eval "db.logs_aggregate.drop()"
#Set your HADOOP_HOME directory here.
export HADOOP_HOME="/Users/mike/hadoop/hadoop-2.0.0-cdh4.3.0" 

#FIRST PASS - map all the devices into an output collection
declare -a job1_args
job1_args=("jar" "/Users/mike/projects/mongo-hadoop/examples/sensors/target/sensors_cdh4.3.0-1.1.0.jar")
#job1_args=(${job1_args[@]} "com.mongodb.hadoop.examples.sensors.Sensors")
job1_args=(${job1_args[@]} "-D" "mongo.job.input.format=com.mongodb.hadoop.MongoInputFormat")
job1_args=(${job1_args[@]} "-D" "mongo.input.uri=mongodb://localhost:27017/demo.devices")
job1_args=(${job1_args[@]} "-D" "mongo.job.mapper=com.mongodb.hadoop.examples.sensors.DeviceMapper")
job1_args=(${job1_args[@]} "-D" "mongo.job.reducer=com.mongodb.hadoop.examples.sensors.DeviceReducer")

job1_args=(${job1_args[@]} "-D" "mongo.job.output.key=org.apache.hadoop.io.Text")
job1_args=(${job1_args[@]} "-D" "mongo.job.output.value=org.apache.hadoop.io.Text")

job1_args=(${job1_args[@]} "-D" "mongo.output.uri=mongodb://localhost:27017/demo.logs_aggregate")
job1_args=(${job1_args[@]} "-D" "mongo.job.output.format=com.mongodb.hadoop.MongoOutputFormat")

$HADOOP_HOME/bin/hadoop "${job1_args[@]}" "$1"

# Create the index that will be used in step 2
mongo demo --eval "db.logs_aggregate.ensureIndex({devices:1})"

#Second PASS - map all the LOGS into the output collection
declare -a job2_args
job2_args=("jar" "/Users/mike/projects/mongo-hadoop/examples/sensors/target/sensors_cdh4.3.0-1.1.0.jar")
#job2_args=(${job2_args[@]} "com.mongodb.hadoop.examples.sensors.Sensors")
job2_args=(${job2_args[@]} "-D" "mongo.job.input.format=com.mongodb.hadoop.BSONFileInputFormat")
job2_args=(${job2_args[@]} "-D" "mapred.input.dir=file:///Users/mike/dump/demo/logs.bson")
job2_args=(${job2_args[@]} "-D" "mongo.job.mapper=com.mongodb.hadoop.examples.sensors.LogMapper")
job2_args=(${job2_args[@]} "-D" "mongo.job.reducer=com.mongodb.hadoop.examples.sensors.LogReducer")
job2_args=(${job2_args[@]} "-D" "mapreduce.combiner.class=com.mongodb.hadoop.examples.sensors.LogCombiner")
job2_args=(${job2_args[@]} "-D" "mongo.job.combiner=com.mongodb.hadoop.examples.sensors.LogCombiner")
job2_args=(${job2_args[@]} "-D" "io.sort.mb=100")

job2_args=(${job2_args[@]} "-D" "mongo.job.output.key=org.apache.hadoop.io.Text")
job2_args=(${job2_args[@]} "-D" "mongo.job.output.value=org.apache.hadoop.io.IntWritable")

job2_args=(${job2_args[@]} "-D" "mongo.output.uri=mongodb://localhost:27017/demo.logs_aggregate")
job2_args=(${job2_args[@]} "-D" "mongo.job.output.format=com.mongodb.hadoop.MongoOutputFormat")

$HADOOP_HOME/bin/hadoop "${job2_args[@]}" "$1"

