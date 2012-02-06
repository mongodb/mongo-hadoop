MongoDB Hadoop Adapter
=======================


We have tested with, and recommend the use of, Hadoop 0.20.203 or Cloudera CHD3 Update 1 (Which ships 0.20.2).  If you wish to use Hadoop Streaming with MongoDB, please see the notes on Streaming Hadoop versions below.

The latest builds are tested primarily against MongoDB 1.8+ but should still work with 1.6.x

*NOTE*: If you have questions please email the [mongodb-user Mailing List](http://groups.google.com/group/mongodb-user), rather than directly contacting contributors or maintainers.

Maintainers
------------
* Brendan McAdams <brendan@10gen.com>
* Eliot Horowitz <erh@10gen.com>
* Ryan Nitz

Contributors
------------
* Sarthak Dudhara <sarthak.83@gmail.com> (BSONWritable comparable interface)
* Priya Manda <priyakanth024@gmail.com> (Test Harness Code)
* Rushin Shah <rushin10@gmail.com> (Test Harness Code)
* Joseph Shraibman <jks@iname.com> (Sharded Input Splits)
* Sumin Xia <xiasumin1984@gmail.com> (Sharded Input Splits)

Using the Adapter
---------------------

You will need the MongoDB Java Driver 2.6.3+.

Issue tracking: https://jira.mongodb.org/browse/HADOOP/

Discussion: http://groups.google.com/group/mongodb-user/

The following features are presently supported:

### Hadoop MapReduce
Working Input and Output adapters for MongoDB are provided.
These can be configured by XML or programatically - see the WordCount
examples for demonstrations of both approaches.
You can specify a query, fields and sort specs in the XML config as JSON
or programatically as a DBObject.

#### Splitting up MongoDB Source Data for the InputFormat
Mongo-Hadoop supports the creation of multiple InputSplits on source data read from MongoDB to optimise/parallelise input processing for Mappers.

If '*mongo.input.split.create_input_splits*' is **false** (it defaults to **true**) then NO splits are used. Hadoop will slurp your entire collection in as one big giant Input.  Mostly useful for debugging.

If it is enabled (by default it is) then a few possible behaviors exist:

  1. If the source collection is unsharded, we follow the "unsharded split" path (See below)  
  2. If the source collection is sharded...
    * If '*mongo.input.split.read_shard_chunks*' is enabled (defaults **true**) then we pull the chunk specs from the configuration server, and turn each shard chunk into an Input Split.  Basically, this means the mongodb sharding system does 99% of the preconfig work for us and is a good thing™
    * If '*read_shard_chunks*' is disabled and '*mongo.input.split.read_from_shards*' is enabled (it defaults to **false**) then we connect to each shard (mongod or replica set) individually and each shard becomes an input split (it's entire content of that collection is one split).  This config should rarely be used.
    * If '*read_shard_chunks*' is enabled and '*mongo.input.split.read_from_shards*' is enabled (it defaults to **false**) we read the chunks from the config server but then instead of reading chunks through mongos we read directly from the shards.  This seems at first like a good idea but if migrations, etc happen it can cause erratic behavior with chunks going away on a long job.  Not a recommended config for write heavy applications but could be nicely parallelising for read heavy apps.
    * If both '*create_input_splits*' and '*read_from_shards*' are disabled then we pretend there is no sharding and use the "unsharded split" path, letting MongoHadoop calculate new splits which are read through mongos (if 'read_shard_chunks' is disabled we just slurp everything through mongos as a single split)

##### "Unsharded Splits"

As aforementioned this can also be used in Sharding.  It refers to a system in which MongoHadoop calculates new splits.

This is only used when '*mongo.input.split.create_input_splits*' is enabled for unsharded collections, and both enabled and '*read_shard_chunks*' disabled for sharded collections.

In this case, MongoHadoop will generate multiple InputSplits.  The user has control over two factors in this system.

* *mongo.input.split_size* - Indicates a number of megabytes that are the maximum size of each split.  I chose 8 as a default for now based on some assumptions and past experience with Hadoop in what will be a good default split size to feed in (the mongo default of 64 may be a bit too large).
* *mongo.input.split.split_key_pattern* - Is a Mongo Key Pattern following [the same rules as picking a shard key](http://www.mongodb.org/display/DOCS/Sharding+Introduction#ShardingIntroduction-ShardKeys)
 for an existing Mongo collection (must be indexed, etc etc).  This will be used as the key for the split point.  It defaults to `{ _id: 1 }` but an experienced user may find it easy to optimize their mapper distribution by customizing this value.

For all three paths, users may specify a custom query to filter the input data with *mongo.input.query* representing a JSON document.  This will be properly combined with the index filtering on input splits allowing you to MapReduce a subset of your data but still get efficient splitting.


### Pig
The MongoStorage Pig module is provided; it currently only supports _saving_ to MongoDB.
Load support will be provided at a later date.

Examples
----------
### WordCount

There are two example WordCount processes for Hadoop MapReduce in `examples/wordcount`
Both read strings from MongoDB and save the count of word frequency.

They are configured to read documents in db `test`, collection `in`, where the string to
count frequency of is defined in field `x`.

The results will be saved in db `test`, collection `out`.

`WordCount.java` is a programatically configured MapReduce job, where all of the configuration
params are setup in the Java code.  You can run this with the ant task `wordcount`.

`WordCountXMLConfig.java` is configured purely through XML files, with JSON for queries, etc.
See examples/wordcount/src/main/resources/mongo-wordcount.xml for the example configuration.
You can run this with the ant task `wordcountXML`, or with a hadoop command of:

    hadoop jar core/target/mongo-hadoop-core-1.0-SNAPSHOT.jar com.mongodb.hadoop.examples.WordCountXMLConfig -conf examples/wordcount/src/main/resources/mongo-wordcount.xml

You will need to copy the `mongo-java-driver.jar` file into your Hadoop `lib` directory before this will work.

### Treasury Yield

The treasury yield example demonstrates working with a more complex input BSON document and calculating an average.

It uses a database of daily US Treasury Bid Curves from 1990 to Sept. 2010 and runs them through to calculate annual averages.

There is a JSON file `examples/treasury_yield/src/main/resources/yield_historical_in.json` which you should import into the `yield_historical.in` collection in the `demo` db.

The sample data can be imported into the mongos host by calling (assumes mongos running on 27017 on the same node):

    mongoimport --db demo --collection yield_historical.in --type json --file examples/treasury_yield/src/main/resources/yield_historical_in.json

You'll need to setup the mongo-hadoop and mongo-java-driver jars in your Hadoop installations "lib" directory; Once the data is imported, the test can be run by executing (on the Hadoop master):

    hadoop jar core/target/mongo-hadoop-core-1.0-SNAPSHOT.jar com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig -conf examples/treasury_yield/src/main/resources/mongo-treasury_yield.xml

To confirm the test ran successfully, look at the `demo` database and query the `yield_historical.out collection`.

###Pig

We presently provide a modified version of the Pig Tutorial from the Pig distribution for testing.

This script differs from the pig tutorial in that it saves the job results to MongoDB.

The use of Pig assumes you have Hadoop & Pig installed and setup on your machine...

Make sure you've built using `ant jar` and then run:


    pig -x local examples/test.pig


You should find the results in the 'test' DB inside the 'pig.output' collection.

TODO
----------
- Pick reasonable split points for non-sharded collections
  * For initial release, no splits for non-sharded collections
- [Elastic map/reduce support?](http://aws.amazon.com/elasticmapreduce/faqs)
- Support for "Merge" Jobs (e.g. combine results of multiple map/reduces esp. from diff. inputs in a single join job - this is supported by Hadoop w/ implementation of special interfaces)
- Full support for appropriate 'alternate' Hadoop Interfaces
  * We already support Pig for Output (get input working)
  * [Cascading](http://www.cascading.org/) Seems to be popular as well and should be evaluated


KNOWN ISSUES
--------------

You cannot configure bare regexes (e.g. /^foo/) in the config xml as they won't parse.
Use {"$regex": "^foo", "$options": ""} instead. .. Make sure to omit the slashes.


STREAMING
----------

Streaming support + MongoDB **requires** your Hadoop distribution include the patches for the following issues:

* [HADOOP-1722 - Make streaming to handle non-utf8 byte array](https://issues.apache.org/jira/browse/HADOOP-1722)
* [HADOOP-5450 - Add support for application-specific typecodes to typed bytes](https://issues.apache.org/jira/browse/HADOOP-5450)
* [MAPREDUCE-764 - TypedBytesInput's readRaw() does not preserve custom type codes](https://issues.apache.org/jira/browse/MAPREDUCE-764)

For the mainline Apache Hadoop distribution, these patches were merged for the 0.21.0 release.  We have verified as well that the [Cloudera](http://cloudera.com) distribution (while based on 0.20.x still) includes these patches in CDH3 Update 1+ (We build against Update 3 now); anecdotal evidence (which needs confirmation) indicates they may have been there since CDH2, and likely exist in CDH3 as well.


By default, The Mongo-Hadoop project builds against Apache 0.20.203 which does *not* include these patches.  To build/enable Streaming support you must build against either Cloudera CDH3u1 or Hadoop 0.21.0; you can change the Hadoop version of the build in Maven by specifying the `hadoop.release` property:

        mvn -Dhadoop.release=cdh3 
        mvn -Dhadoop.release=cloudera

Will both build against Cloudera CDH3u3, while:


        mvn -Dhadoop.release=apache-hadoop-0.21


Will build against Hadoop 0.21 from the mainline Apache distribution.  Unfortunately we are not aware of any Maven Repositories which currently contain artifacts for Hadoop 0.21, and you may need to resolve these dependencies by hand if you choose to go down the 'Vanilla' route.

Additionally, note that Hadoop 1.0 is based on the 0.20 release.  As such, it *does not include* the patches necessary for streaming.  This is frustrating and upsetting but unfortunately out of our hands.  We are working on attempting to get these patches backported into a future release or finding an additional workaround.
