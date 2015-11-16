1.4.2 / 16th of November, 2015
==============================

This is a minor release that contains small improvements and bug fixes from 1.4.1.

  * [HADOOP-238] Run the 'splitVector' command on the same database from which we want the splits.

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
