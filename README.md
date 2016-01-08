#MongoDB Connector for Hadoop

##Purpose

The MongoDB Connector for Hadoop is a library which allows MongoDB (or backup files in its data format, BSON) to be used as an input source, or output destination, for Hadoop MapReduce tasks. It is designed to allow greater flexibility and performance and make it easy to integrate data in MongoDB with other parts of the Hadoop ecosystem including the following:
* [**Pig**][pig-usage]
* [**Spark**][spark-usage]
* [**MapReduce**][mapreduce-usage]
* [**Hadoop Streaming**][streaming-usage]
* [**Hive**][hive-usage]
* [**Flume**][flume-usage]

Check out the [releases](https://github.com/mongodb/mongo-hadoop/releases) page for the latest stable release.

## Features

* Can create data splits to read from standalone, replica set, or sharded configurations
* Source data can be filtered with queries using the MongoDB query language
* Supports Hadoop Streaming, to allow job code to be written in any language (python, ruby, nodejs currently supported)
* Can read data from MongoDB backup files residing on S3, HDFS, or local filesystems
* Can write data out in .bson format, which can then be imported to any MongoDB database with `mongorestore`
* Works with BSON/MongoDB documents in other Hadoop tools such as [**Pig**][pig-usage] and [**Hive**][hive-usage].

## Download
See the [release](https://github.com/mongodb/mongo-hadoop/releases) page.

## Building

Run `./gradlew jar` to build the jars.  The jars will be placed in to `build/libs` for each module.  e.g. for the core module, 
it will be generated in the `core/build/libs` directory.

After successfully building, you must copy the jars to the lib directory on each node in your hadoop cluster. This is usually one of the
following locations, depending on which Hadoop release you are using:

* `$HADOOP_PREFIX/lib/`
* `$HADOOP_PREFIX/share/hadoop/mapreduce/`
* `$HADOOP_PREFIX/share/hadoop/lib/`

mongo-hadoop should work on any distribution of hadoop.  Should you run in to an issue, please file a 
[Jira](https://jira.mongodb.org/browse/HADOOP/) ticket.

## Documentation

For full documentation, please check out the [Hadoop Connector Wiki][wiki]. The documentation includes installation instructions, configuration options, as well as specific instructions and examples for each Hadoop application the connector supports.

## Usage with Amazon Elastic MapReduce

Amazon Elastic MapReduce is a managed Hadoop framework that allows you to submit jobs to a cluster of customizable size and configuration,
without needing to deal with provisioning nodes and installing software.

Using EMR with the MongoDB Connector for Hadoop allows you to run MapReduce jobs against MongoDB backup files stored in S3.

Submitting jobs using the MongoDB Connector for Hadoop to EMR simply requires that the bootstrap actions fetch the dependencies (mongoDB 
java driver, mongo-hadoop-core libs, etc.) and place them into the hadoop distributions `lib` folders.

For a full example (running the enron example on Elastic MapReduce) please see [here](https://github.com/mongodb/mongo-hadoop/wiki/Enron-Emails-Example).

## Notes for Contributors

If your code introduces new features, add tests that cover them if possible and make sure that `./gradlew check` still passes. For instructions on how to run the tests, see the [Running the Tests](https://github.com/mongodb/mongo-hadoop/wiki/Running-the-Tests) section in the [wiki][wiki].
If you're not sure how to write a test for a feature or have trouble with a test failure, please post on the google-groups with details 
and we will try to help.  _Note_: Until findbugs updates its dependencies, running `./gradlew check` on Java 8 will fail.

### Maintainers
Luke Lovett (luke.lovett@mongodb.com)

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
* Justin Lee
* Luke Lovett
* Mariano Semelman

### Support

Issue tracking: https://jira.mongodb.org/browse/HADOOP/

Discussion: http://groups.google.com/group/mongodb-user/

[pig-usage]: https://github.com/mongodb/mongo-hadoop/wiki/Pig-Usage
[hive-usage]: https://github.com/mongodb/mongo-hadoop/wiki/Hive-Usage
[flume-usage]: https://github.com/mongodb/mongo-hadoop/wiki/Flume-Usage
[streaming-usage]: https://github.com/mongodb/mongo-hadoop/wiki/Streaming-Usage
[spark-usage]: https://github.com/mongodb/mongo-hadoop/wiki/Spark-Usage
[mapreduce-usage]: https://github.com/mongodb/mongo-hadoop/wiki/MapReduce-Usage
[wiki]: https://github.com/mongodb/mongo-hadoop/wiki
