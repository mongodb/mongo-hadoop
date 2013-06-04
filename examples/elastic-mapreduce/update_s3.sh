#!/bin/sh

#Take the enron example jars and put them into an S3 bucket.

s3cp ./emr-bootstrap.sh s3://$S3_BUCKET/emr-bootstrap.sh
s3mod s3://$S3_BUCKET/emr-bootstrap.sh public-read
s3cp ../core/target/mongo-hadoop-core_1.0.4-1.1.0.jar s3://$S3_BUCKET/mongo-hadoop-core_1.0.4-1.1.0.jar
s3mod s3://$S3_BUCKET/mongo-hadoop-core_1.0.4-1.1.0.jar public-read
s3cp ../examples/enron/target/enron-example_1.0.4-1.1.0.jar core/target/ s3://$S3_BUCKET/enron-example_1.0.4-1.1.0.jar
s3mod s3://$S3_BUCKET/enron-example_1.0.4-1.1.0.jar public-read
