#!/bin/sh

cd ..
./sbt mongo-hadoop-streaming/assembly
cd -
cp target/mongo-hadoop-streaming-assembly*.jar mongo-hadoop-streaming.jar
