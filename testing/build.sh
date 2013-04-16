#!/bin/sh

#set HADOOP_HOME and HADOOPVER first and run this script, like this:
#HADOOP_HOME=~/hadoop/hadoop-1.0.4 HADOOPVER="1.0.x" ./build.sh

sed "s/hadoopRelease.*/hadoopRelease in ThisBuild := \"$HADOOPVER\"/g" ../build.sbt > ../build2.sbt
mv ../build2.sbt ../build.sbt
rm ../build2.sbt
cd ..
./sbt package
./sbt treasury-example/package
rm $HADOOP_HOME/lib/mongo-hadoop*.jar
rm $HADOOP_HOME/share/hadoop/mapreduce/mongo-hadoop*.jar 
rm $HADOOP_HOME/share/hadoop/lib/mongo-hadoop*.jar 
cp core/target/mongo-hadoop-core_*.jar $HADOOP_HOME/lib/
cp target/mongo-hadoop_*.jar $HADOOP_HOME/lib/

# 0.20 
cp core/target/mongo-hadoop-core_*.jar $HADOOP_HOME/share/hadoop/lib/
cp target/mongo-hadoop_*.jar $HADOOP_HOME/share/hadoop/lib/

# 0.20 
cp core/target/mongo-hadoop-core_*.jar $HADOOP_HOME/share/hadoop/mapreduce/
cp target/mongo-hadoop_*.jar $HADOOP_HOME/share/hadoop/mapreduce/




