#!/bin/sh

#Take the enron example jars and put them into an S3 bucket.
HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

s3cp $HERE/emr-bootstrap.sh s3://$S3_BUCKET/emr-bootstrap.sh
s3mod s3://$S3_BUCKET/emr-bootstrap.sh public-read
s3cp $HERE/../../core/target/mongo-hadoop-core_1.1.2-1.1.0.jar s3://$S3_BUCKET/mongo-hadoop-core_1.1.2-1.1.0.jar
s3mod s3://$S3_BUCKET/mongo-hadoop-core_1.1.2-1.1.0.jar public-read
s3cp $HERE/../enron/target/enron-example_1.1.2-1.1.0.jar s3://$S3_BUCKET/enron-example_1.1.2-1.1.0.jar
s3mod s3://$S3_BUCKET/enron-example_1.1.2-1.1.0.jar public-read
