#!/bin/sh


#HADOOPVER="0.23.x"
HADOOPVER="1.0.x"
sed "s/hadoopRelease.*/hadoopRelease in ThisBuild := \"$HADOOPVER\"/g" ../build.sbt > ../build2.sbt
mv ../build2.sbt ../build.sbt
cd ..
#./sbt clean
./sbt package
./sbt treasury-example/package
#export HADOOP_HOME="/Users/mike/hadoop/hadoop-0.20.2-cdh3u5"
export HADOOP_HOME="/Users/mike/hadoop/hadoop-1.0.4"
#/hadoop-1.0.4"
    
#
rm $HADOOP_HOME/lib/mongo-hadoop*.jar
cp core/target/mongo-hadoop-core_*.jar $HADOOP_HOME/lib/
cp target/mongo-hadoop_*.jar $HADOOP_HOME/lib/
#$HADOOP_HOME/bin/hadoop jar examples/treasury_yield/target/treasury-example_*.jar com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig
#target/mongo-hadoop_0.20.205.0-1.1.0-SNAPSHOT.jar flume/target/mongo-flume-1.1.0-SNAPSHOT.jar examples/treasury_yield/target/treasury-example_0.20.205.0-1.1.0-SNAPSHOT.jar pig/target/mongo-hadoop-pig_0.20.205.0-1.1.0-SNAPSHOT.jar





