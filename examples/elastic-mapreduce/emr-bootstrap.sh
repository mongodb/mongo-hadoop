#!/bin/sh

wget -P /home/hadoop/lib http://central.maven.org/maven2/org/mongodb/mongo-java-driver/2.11.1/mongo-java-driver-2.11.1.jar
wget -P /home/hadoop/lib https://s3.amazonaws.com/$S3_BUCKET/mongo-hadoop-core_1.0.4-1.1.0.jar
