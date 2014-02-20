#!/bin/sh

wget -P /home/hadoop/lib http://central.maven.org/maven2/org/mongodb/mongo-java-driver/2.11.1/mongo-java-driver-2.11.1.jar

# Edit this path to point to the location of the jar you're using.
wget -P /home/hadoop/lib https://s3.amazonaws.com/mongo-hadoop-code/mongo-hadoop-core_1.1.2-1.1.0.jar
