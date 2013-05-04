#!/bin/sh

#set HADOOP_HOME and HADOOPVER first and run this script, like this:
#HADOOP_HOME=~/hadoop/hadoop-1.0.4 HADOOPVER="1.0.x" ./build.sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECTROOT="$DIR/.."


sed "s/hadoopRelease.*/hadoopRelease in ThisBuild := \"$HADOOPVER\"/g" $PROJECTROOT/build.sbt > $PROJECTROOT/build2.sbt
mv $PROJECTROOT/build2.sbt $PROJECTROOT/build.sbt
rm $PROJECTROOT/build2.sbt
cd $PROJECTROOT
./sbt package
./sbt treasury-example/package
./sbt enron-example/package
./sbt mongo-hadoop-streaming/assembly
rm $HADOOP_HOME/lib/mongo-hadoop*.jar
rm $HADOOP_HOME/share/hadoop/mapreduce/mongo-hadoop*.jar 
rm $HADOOP_HOME/share/hadoop/lib/mongo-hadoop*.jar 
cp $PROJECTROOT/core/target/mongo-hadoop-core_*.jar $HADOOP_HOME/lib/
cp $PROJECTROOT/target/mongo-hadoop_*.jar $HADOOP_HOME/lib/

# 0.20 
cp $PROJECTROOT/core/target/mongo-hadoop-core_*.jar $HADOOP_HOME/share/hadoop/lib/
cp $PROJECTROOT/target/mongo-hadoop_*.jar $HADOOP_HOME/share/hadoop/lib/

# 0.20 
cp $PROJECTROOT/core/target/mongo-hadoop-core_*.jar $HADOOP_HOME/share/hadoop/mapreduce/
cp $PROJECTROOT/target/mongo-hadoop_*.jar $HADOOP_HOME/share/hadoop/mapreduce/

cp $PROJECTROOT/streaming/target/mongo-hadoop-streaming*.jar $HADOOP_HOME/share/hadoop/mapreduce/



