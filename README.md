MongoDB Hadoop Adapter
=======================

PLEASE BE CAREFUL WITH THIS IN PRODUCTION
------------------------------------------
We are still shaking out bugs and features, so make sure you test this in your setup before deploying it to production.

We have tested with, and recommend the use of, Hadoop 0.20.203 or Cloudera CHD3 (Which ships 0.20.2)

The latest builds are tested primarily against MongoDB 1.8+ but should still work with 1.6.x

NOTE: We have not tested this driver with Hadoop 0.21 yet.  Hadoop lists 0.21 as "unstable, unsupported " and until that changes we will not provide any full support.

Maintainers
------------
* Brendan McAdams <brendan@10gen.com>
* Eliot Horowitz <erh@10gen.com>
* Ryan Nitz <ryan@10gen.com>

Contributors
------------
* Sarthak Dudhara <sarthak.83@gmail.com> (BSONWritable comparable interface)
* Priya Manda <priyakanth024@gmail.com> (Test Harness Code)
* Rushin Shah <rushin10@gmail.com> (Test Harness Code)
* Joseph Shraibman <jks@iname.com> (Sharded Input Splits)
* Sumin Xia <xiasumin1984@gmail.com> (Sharded Input Splits)

State of the Adapter
---------
This is currently under development and is not feature complete.

However, the core functionality is relatively stable

It should be considered an early beta.

You will need the MongoDB Java Driver 2.5.3+ or `master`.

Issue tracking: https://github.com/mongodb/mongo-hadoop/issues

Discussion: http://groups.google.com/group/mongodb-user/

The following features are presently supported:

### Hadoop MapReduce
Working Input and Output adapters for MongoDB are provided.
These can be configured by XML or programatically - see the WordCount
examples for demonstrations of both approaches.
You can specify a query, fields and sort specs in the XML config as JSON
or programatically as a DBObject.

Sharding is currently NOT supported explicitly (e.g. we don't use the chunks
to read from individual shards).

There are presently NO input splits - your entire collection is passed as a single
split to a single mapper. If you have a problem which requires more discreet splits
please email us to describe your problem

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
See examples/wordcount/resources/mongo-wordcount.xml for the example configuration.
You can run this with the ant task `wordcountXML`, or with a hadoop command of:

    hadoop jar mongo-hadoop.jar com.mongodb.hadoop.examples.WordCountXMLConfig -conf examples/wordcount/resources/mongo-wordcount.xml

You will need to copy the `mongo-java-driver.jar` file into your Hadoop `lib` directory before this will work.

### Treasury Yield

The treasury yield example demonstrates working with a more complex input BSON document and calculating an average.

It uses a database of daily US Treasury Bid Curves from 1990 to Sept. 2010 and runs them through to calculate annual averages.

There is a JSON file `examples/treasury_yield/resources/yield_historical_in.json` which you should import into the `yield_historical.in` collection in the `demo` db.

The sample data can be imported into the mongos host by calling (assumes mongos running on 27017 on the same node):

    mongoimport --db demo --collection yield_historical.in --type json --file examples/treasury_yield/resources/yield_historical_in.json

Once the data is imported, the test can be run by executing (on the Hadoop master):

    hadoop jar mongo-hadoop.jar com.mongodb.hadoop.examples.TreasuryYieldXMLConfig -conf examples/treasury_yield/resources/mongo-treasury_yield.xml

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

Streaming is a work in progress as we are sorting out how to most efficiently stream binary data.

Essentially, Streaming support + MongoDB **requires** your Hadoop distribution include the patches for the following issues:

    * [HADOOP-1722 - Make streaming to handle non-utf8 byte array](https://issues.apache.org/jira/browse/HADOOP-1722)
    * [HADOOP-5450 - Add support for application-specific typecodes to typed bytes](https://issues.apache.org/jira/browse/HADOOP-5450)
    * [MAPREDUCE-764 - TypedBytesInput's readRaw() does not preserve custom type codes](https://issues.apache.org/jira/browse/MAPREDUCE-764)

For the mainline Apache Hadoop distribution, these patches were merged for the 0.21.0 release.  We have verified as well that the [Cloudera](http://cloudera.com) distribution (while based on 0.20.x still) includes these patches in CDH3 Update 1; anecdotal evidence (which needs confirmation) indicates they may have been there since CDH2, and likely exist in CDH3 as well.

Running Streaming:
  hadoop jar $HADOOP_STREAMING -conf examples/treasury_yield/resources/mongo-treasury_yield.xml -libjars mongo-hadoop.jar,lib/mongo-java-driver-2.4.jar  -mapper examples/treasury_yield/src/mapper.py -reducer examples/treasury_yield/src/reducer.py -inputformat com.mongodb.hadoop.mapred.MongoInputFormat -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat -input README.md -output foo.md

You will need Hadoop Streaming 0.21 or higher to make this work --- 0.20.2 does *not* support Binary streaming


  hadoop jar $HADOOP_STREAMING -D stream.map.input=rawbytes -conf examples/treasury_yield/resources/mongo-treasury_yield.xml -libjars mongo-hadoop.jar,lib/mongo-java-driver-2.4.jar  -mapper examples/treasury_yield/src/mapper.py -reducer examples/treasury_yield/src/reducer.py -inputformat com.mongodb.hadoop.mapred.MongoInputFormat -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat -input README.md -output foo.md

  hadoop jar $HADOOP_STREAMING -conf examples/treasury_yield/resources/mongo-treasury_yield.xml -libjars mongo-hadoop.jar,lib/mongo-java-driver-2.4.jar  -mapper examples/treasury_yield/src/mapper.py -reducer examples/treasury_yield/src/reducer.py -inputformat com.mongodb.hadoop.mapred.MongoInputFormat -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat -input README.md -output foo.md -io rawbytes

hadoop jar $HADOOP_STREAMING -libjars mongo-hadoop.jar,lib/mongo-java-driver-2.4.jar -mapper examples/treasury_yield/src/mapper.py -reducer examples/treasury_yield/src/reducer.py -inputformat com.mongodb.hadoop.mapred.MongoInputFormat -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat -input mongodb://localhost/demo.yield_historical.in -output mongodb://localhost/demo.yield_historical.out

hadoop jar ./mongo-hadoop-streaming.jar -libjars mongo-hadoop.jar,lib/mongo-java-driver-2.4.jar -mapper examples/treasury_yield/src/mapper.py -reducer examples/treasury_yield/src/reducer.py -inputformat com.mongodb.hadoop.mapred.MongoInputFormat -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat -input mongodb://localhost/demo.yield_historical.in -output mongodb://localhost/demo.yield_historical.out

hadoop jar mongo-hadoop-streaming.jar -libjars $HADOOP_STREAMING,mongo-hadoop.jar,lib/mongo-java-driver-2.4.jar -mapper examples/treasury_yield/src/mapper.py -reducer examples/treasury_yield/src/reducer.py -inputformat com.mongodb.hadoop.mapred.MongoInputFormat -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat -input mongodb://localhost/demo.yield_historical.in -output mongodb://localhost/demo.yield_historical.out
