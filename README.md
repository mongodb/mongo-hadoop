#Mongo-Hadoop Adapter

##Purpose

The mongo-hadoop adapter is a library which allows MongoDB (or backup files in its data format, BSON) to be used as an input source, or output destination, for Hadoop MapReduce tasks. It is designed to allow greater flexibility and performance and make it easy to integrate data in MongoDB with other parts of the Hadoop ecosystem. 

Current release: **1.1**

## Features

* Can create data splits to read from standalone, replica set, or sharded configurations
* Source data can be filtered with queries using the MongoDB query language
* Supports Hadoop Streaming, to allow job code to be written in any language (python, ruby, nodejs currently supported)
* Can read data from MongoDB backup files residing on s3, hdfs, or local filesystems
* Can write data out in .bson format, which can then be imported to any mongo database with `mongorestore`
* Work with BSON/MongoDB documents in other Hadoop tools such as **Pig** and **Hive**.

## Download

* TODO List links to binaries here.
* TODO List maven artifacts

## Building

To build, first edit the value for `hadoopRelease in ThisBuild` in the build.sbt file to select the distribution of Hadoop that you want to build against. For example to build for CDH4:

    hadoopRelease in ThisBuild := "cdh4"

or for Hadoop 1.0.x:

    hadoopRelease in ThisBuild := "1.0"

To figure out which value you need to set in this file, refer to the list of distributions below.
Then run `./sbt package` to build the jars, which will be generated in the `target/` directory.

After successfully building, you must copy the jars to the lib directory on each node in your hadoop cluster. This is usually one of the following locations, depending on which Hadoop release you are using:

* `$HADOOP_HOME/lib/`
* `$HADOOP_HOME/share/hadoop/mapreduce/`
* `$HADOOP_HOME/share/hadoop/lib/`


## Supported Distributions of Hadoop

* ###Apache Hadoop 1.0
   Does **not** support Hadoop Streaming.

   Build using `"1.0"` or `"1.0.x"`

* ###Apache Hadoop 0.20.*
   Does **not** support Hadoop Streaming
   
   Includes Pig 0.9.2.
   
   Build using `"0.20"` or `"0.20.x"`
   
* ###Apache Hadoop 0.23
   Includes Pig 0.9.2.
   
   Includes support for Streaming
   
   Build using `"0.23"` or `"0.23.x"`

* ###Apache Hadoop 0.21
   Includes Pig 0.9.1
   
   Includes support for Streaming
   
   Build using `"0.21"` or `"0.21.x"`

* ###Cloudera Hadoop Release 3
    This is derived from Apache Hadoop 0.20.2 and includes custom patches.
    
   Includes support for streaming and Pig 0.8.1.

   Build with `"cdh3"`

* ###Cloudera Hadoop Release 4
 
   This is the newest release from Cloudera which is based on Apache Hadoop 2.0. The newer MR2/YARN APIs are not yet supported, but MR1 is still fully compatible.
   
   Includes support for Streaming and Pig 0.11.1.
   
   Build with `"cdh4"`

## Configuration

[Configuration](CONFIG.md)

## Streaming

[Streaming](streaming/README.md)

## Examples

[Examples](examples/README.md)

## Usage with static .bson (mongo backup) files

[BSON Usage](examples/BSON_README.md)

## Usage with Amazon Elastic MapReduce

Amazon Elastic MapReduce is a managed Hadoop framework that allows you to submit jobs to a cluster of customizable size and configuration, without needing to deal with provisioning nodes and installing software.

Using EMR with mongo-hadoop allows you to run MapReduce jobs against mongo backup files stored in S3.

Submitting jobs using mongo-hadoop to EMR simply requires that the bootstrap actions fetch the dependencies (mongoDB java driver, mongo-hadoop-core libs, etc.) and place them into the hadoop distributions `lib` folders.

For a full example (running the enron example on Elastic MapReduce) please see [here](examples/elastic-mapreduce/README.md).

## Usage with Pig

[Documentation on Pig with Mongo-Hadoop](pig/README.md).

For examples on using Pig with mongo-hadoop, also refer to the [examples section](examples/README.md).

## Notes for Contributors

If your code introduces new features, please add tests that cover them if possible and make sure that the existing test suite  still passes. If you're not sure how to write a test for a feature or have trouble with a test failure, please post on the google-groups with details and we will try to help. 

### Maintainers
Mike O'Brien (mikeo@10gen.com)

### Contributors
* Brendan McAdams brendan@10gen.com
* Eliot Horowitz erh@10gen.com
* Ryan Nitz ryan@10gen.com
* Russell Jurney (@rjurney) (Lots of significant Pig improvements)
* Sarthak Dudhara sarthak.83@gmail.com (BSONWritable comparable interface)
* Priya Manda priyakanth024@gmail.com (Test Harness Code)
* Rushin Shah rushin10@gmail.com (Test Harness Code)
* Joseph Shraibman jks@iname.com (Sharded Input Splits)
* Sumin Xia xiasumin1984@gmail.com (Sharded Input Splits)

### Support

Issue tracking: https://jira.mongodb.org/browse/HADOOP/

Discussion: http://groups.google.com/group/mongodb-user/

