#!/bin/sh

cd ..
./sbt mongo-hadoop-streaming/assembly
cd -
cp target/mongo-hadoop-streaming-assembly-1.0.0-rc1-SNAPSHOT.jar mongo-hadoop-streaming.jar
