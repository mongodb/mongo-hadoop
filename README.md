#MongoDB Connector for Hadoop

##Purpose

The MongoDB Connector for Hadoop is a library which allows MongoDB (or backup files in its data format, BSON) to be used as an input source, or output destination, for Hadoop MapReduce tasks. It is designed to allow greater flexibility and performance and make it easy to integrate data in MongoDB with other parts of the Hadoop ecosystem.

Current stable release: **1.2.1**

## Features

* Can create data splits to read from standalone, replica set, or sharded configurations
* Source data can be filtered with queries using the MongoDB query language
* Supports Hadoop Streaming, to allow job code to be written in any language (python, ruby, nodejs currently supported)
* Can read data from MongoDB backup files residing on S3, HDFS, or local filesystems
* Can write data out in .bson format, which can then be imported to any MongoDB database with `mongorestore`
* Works with BSON/MongoDB documents in other Hadoop tools such as **Pig** and **Hive**.

## Download
See the [release](https://github.com/mongodb/mongo-hadoop/releases) page.

## Building

The mongo-hadoop connector currently supports the following versions of hadoop:  0.23, 1.0, 1.1, 2.2, 2.3, 2.4, 
and CDH 4 abd 5.  The default build version will build against the last Apache Hadoop (currently 2.4).  If you would like to build 
against a specific version of Hadoop you simply need to pass `-Phadoop_version=<your version>` to gradlew when building.

Run `./gradlew jar` to build the jars.  The jars will be placed in to `build/libs` for each module.  e.g. for the core module, 
it will be generated in the `core/build/libs` directory.

After successfully building, you must copy the jars to the lib directory on each node in your hadoop cluster. This is usually one of the
following locations, depending on which Hadoop release you are using:

* `$HADOOP_HOME/lib/`
* `$HADOOP_HOME/share/hadoop/mapreduce/`
* `$HADOOP_HOME/share/hadoop/lib/`

## Supported Distributions of Hadoop

| Hadoop Version                       | Build Parameter         |
| :----------------------------------: | :---------------------: |
| Apache Hadoop 0.23                   | -Phadoop_version='0.23' |
| Apache Hadoop 1.0                    | -Phadoop_version='1.0'  |
| Apache Hadoop 1.1                    | -Phadoop_version='1.1'  |
| Apache Hadoop 2.2                    | -Phadoop_version='2.2'  |
| Apache Hadoop 2.3                    | -Phadoop_version='2.3'  |
| Apache Hadoop 2.4                    | -Phadoop_version='2.4'  |
| Cloudera Distribution for Hadoop 4   | -Phadoop_version='cdh4' |
| Cloudera Distribution for Hadoop 5   | -Phadoop_version='cdh5' |

## Configuration

[Configuration](CONFIG.md)

## Streaming

[Streaming](streaming/README.md)

## Hive

[Hive](hive/README.md)

## Pig

[Pig](pig/README.md)

## Examples

[Examples](examples/README.md)

## Usage with static .bson (mongo backup) files

[BSON Usage](BSON_README.md)

## Usage with Amazon Elastic MapReduce

Amazon Elastic MapReduce is a managed Hadoop framework that allows you to submit jobs to a cluster of customizable size and configuration,
without needing to deal with provisioning nodes and installing software.

Using EMR with the MongoDB Connector for Hadoop allows you to run MapReduce jobs against MongoDB backup files stored in S3.

Submitting jobs using the MongoDB Connector for Hadoop to EMR simply requires that the bootstrap actions fetch the dependencies (mongoDB 
java driver, mongo-hadoop-core libs, etc.) and place them into the hadoop distributions `lib` folders.

For a full example (running the enron example on Elastic MapReduce) please see [here](examples/elastic-mapreduce/README.md).

## Usage with Pig

[Documentation on Pig with the MongoDB Connector for Hadoop](pig/README.md).

For examples on using Pig with the MongoDB Connector for Hadoop, also refer to the [examples section](examples/README.md).

## Notes for Contributors

If your code introduces new features, add tests that cover them if possible and make sure that `./gradlew check` still passes.
If you're not sure how to write a test for a feature or have trouble with a test failure, please post on the google-groups with details 
and we will try to help.  _Note_: Until findbugs updates its dependencies, running `./gradlew check` on Java 8 will fail.

### Maintainers
Justin lee (justin.lee@mongodb.com)

### Contributors
* Mike O'Brien (mikeo@10gen.com)
* Brendan McAdams brendan@10gen.com
* Eliot Horowitz erh@10gen.com
* Ryan Nitz ryan@10gen.com
* Russell Jurney (@rjurney) (Lots of significant Pig improvements)
* Sarthak Dudhara sarthak.83@gmail.com (BSONWritable comparable interface)
* Priya Manda priyakanth024@gmail.com (Test Harness Code)
* Rushin Shah rushin10@gmail.com (Test Harness Code)
* Joseph Shraibman jks@iname.com (Sharded Input Splits)
* Sumin Xia xiasumin1984@gmail.com (Sharded Input Splits)
* Jeremy Karn
* bpfoster
* Ross Lawley
* Carsten Hufe
* Asya Kamsky
* Thomas Millar

### Support

Issue tracking: https://jira.mongodb.org/browse/HADOOP/

Discussion: http://groups.google.com/group/mongodb-user/