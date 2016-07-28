2.0.0-rc0 / 26th of June, 2016
==============================

This is a major release touting several new features. As a major release, it
removes several deprecated methods and objects, breaking API in ways that should
not affect most users of Hadoop tools.

Some of the major new features introduced in this version include:

* Ability to collocate Hadoop nodes and MongoDB shards for data locality (HADOOP-202)
* Add GridFSInputFormat (HADOOP-272)
* Add MongoSampleSplitter (HADOOP-283)
* Support document replacement (HADOOP-263)
* Add back support for Hadoop 1.2.x (HADOOP-246)

For complete details on the issues resolved in 2.0.0, consult the release notes
on Jira: https://jira.mongodb.org/browse/HADOOP/fixforversion/15622

1.5.2 / 28th of March, 2016
===========================

This release fixes a couple issues when using the "pymongo-spark" library,
including a bug where datetimes were being decoded to
java.util.GregorianCalendar and another bug where pymongo-was not working
in non-local Spark setups.

For complete details on the issues resolved in 1.5.2, consult the release notes
on Jira: https://jira.mongodb.org/browse/HADOOP/fixforversion/16602

1.5.1 / 9th of March, 2016
==========================

This release features a few fixes from 1.5.0, including patching a few MongoDB
connection leaks, avoiding a warning when using MongoUpdateStorage with Pig, and
allowing a limit to be set on MongoInputSplits.

For complete details on the issues resolved in 1.5.1, consult the release notes
on Jira: https://jira.mongodb.org/browse/HADOOP/fixforversion/16544

1.5.0 / 23rd of February, 2016
==============================

This release features major improvements to Pig, Hive, and Spark. Pig and Hive
both have the ability to push down simple queries and projections to MongoDB,
potentially saving time and memory when running MapReduce jobs. New included
UDFs allow writing MongoDB-specific types from Pig jobs and extracting timestamp
information from ObjectIds. A new "pymongo-spark" library (under
spark/src/main/python) allows using PyMongo objects with the connector, greatly
simplifying the Python interface to Spark when running with MongoDB.

For a complete list of tickets resolved in this release, see the release notes
on Jira: https://jira.mongodb.org/browse/HADOOP/fixforversion/15466

Changes from rc0:

   * [HADOOP-255] Return null early in getTypeForBSON if input is null.

1.5.0-rc0 / 1st of Febuary, 2016
================================

This release features major improvements to Pig, Hive, and Spark. Pig and Hive
both have the ability to push down simple queries and projections to MongoDB,
potentially saving time and memory when running MapReduce jobs. New included
UDFs allow writing MongoDB-specific types from Pig jobs and extracting timestamp
information from ObjectIds. A new "pymongo-spark" library (under
spark/src/main/python) allows using PyMongo objects with the connector, greatly
simplifying the Python interface to Spark when running with MongoDB.

For a complete list of tickets resolved in this release, see the release notes
on Jira: https://jira.mongodb.org/browse/HADOOP/fixforversion/15466

1.4.1 / 29th of September, 2015
===============================

This is a minor release that contains minor improvements and bug fixes from 1.4.0.

  * [HADOOP-231] (Python) Streaming reports success but output collection stays empty
  * [HADOOP-226] HiveException: java.lang.ClassCastException: java.lang.String cannot be cast to java.sql.Timestamp
  * [HADOOP-219] Do not log the username:password portion of the mongo connection URI to hadoop logs

1.4 / 2nd of July, 2015
=======================

  * [HADOOP-206] Update progress inside MongoOutputCommitter so that Hadoop doesn't time out the commit

This stable release also includes all features and fixes from the 1.4-rc0 release described below.

1.4-rc0 / 18th of June, 2015
============================

  * [HADOOP-204] Allow concurrent access to MongoRecordReader instances
  * [HADOOP-201] Support mongo.auth.uri in StandaloneMongoSplitter
  * [HADOOP-196] Update Hadoop dependencies
  * [HADOOP-195] 3.0 Java driver compatibility
  * [HADOOP-188] Support MapWritable
  * [HADOOP-179] When mongo.output.uri has a replica set specified, mongo-hadoop fails
  * [HADOOP-175] Records dropped due to incorrectly computed file splits
  * [HADOOP-173] Bulk write support from MongoOutputFormat
  * [HADOOP-170] Pig integration doesn't call close() on Client
  * [HADOOP-153] Add capability of BSONLoader.java to parse UUID
  * [HADOOP-152] NumberFormatExceptions when splitting on a sharded, replica set cluster
  * [HADOOP-151] Fix MongoUpdateWritable serialization
  * [HADOOP-150] Use Primary read preference when sending splitVector command in StandaloneMongoSplitter
  * [HADOOP-143] MongoConfigUtil.getCollection() creates orphaned MongoClients
  * [HADOOP-110] Add non-args constructor for all spiltters for multi-collection input
  * [HADOOP-98] handle binary types in pig schema mode
  * [HADOOP-94] BSONLoader failing to parse binary data
  * [HADOOP-93] Processing GUID data when importing data from mongo to HDFS using Pig
  * [HADOOP-82] Use OutputCommitter with MongoOutputFormat

1.0.0 / 2012-04-09 
==================

  * Fixed file distribution for streaming addon files
  * Fixed Thrift dep for cdh3.
  * Add treasury yield example build support.
  * Added a Streaming Example M/R job with enron email corpus
  * HADOOP-29 - removes excessive logging for each tuple stored in MongoDB (RJurney)
  * Streaming: Add support for python generators in reduce functions (MLew)
  * Pig: Fix for exporting tuples to mongodb as map
  * Fixed CDH4 build flags to correct compilation step.
  * Fixed Hadoop build for dependencies across versions.
  * Added a "load-sample-data" task to use for loading samples into mongo for testing/demos
  * Hadoop 0.22.x support now works for those who need it (although I believe it's a deprecated branch)
  * Stock Apache 0.23.x now builds, using the actual 0.23.1 release...  insanity around the MapReduce dep
  * added twitter hashtag examples
  * Relocate Pymongo_Hadoop module to a new "language_Support" subdirectory. Created a setup.py file to build an egg / package. Available on PyPi as 'pymongo_hadoop'.
  * Fixed pymongo_hadoop output to use BSON.encode
  * Added support to streaming for the -file flag to distribute files out to the cluster if they don't exist.
  * Make InputFormat and OutputFormat implied on Streaming jobs, defaulting to the Mongo ones.
  * Streaming now builds as a fat assembly jar and works.
  * Added an 0.23 / cdh4 build.  No longer allow raw "cdh" or "cloudera" build artifacts to avoid confusion as to 'which cloudera?'
  * Added a .23 build, based on Cloudera's current distro (should be binary compatible with stock)
  * If combiner is not specified, do not pass it to Hadoop.  While the combiner should be optional, giving Hadoop a null combiner will result in a NullPointerException.

1.0.0-rc0 / 2012-02-12 
==================

  * Initial Release, Release Candidate
