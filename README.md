MongoDB Hadoop Adapter
=======================

EXPERIMENTAL: DO NOT USE FOR PRODUCTION
---------------------------------------
NOTE: We have not tested this driver with Hadoop 0.21 yet.  We have tested with, and recommend the use of,
Hadoop 0.20.2 or Cloudera CHD3 (Which ships 0.20.2)

State of the Adapter
---------
This is currently under development and is not feature complete.

It should be considered an Alpha.

You will need the MongoDB Java Driver 2.3 or Master.

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

There is a JSON file `examples/treasury_yield/resources/yield_historical_in.json` which you should import into the `test` db in the `yield_historical.in` collection.



###Pig

We presently provide a modified version of the Pig Tutorial from the Pig distribution for testing.

This script differs from the pig tutorial in that it saves the job results to MongoDB.

The use of Pig assumes you have Hadoop & Pig installed and setup on your machine...

Make sure you've built using `ant jar` and then run:


    pig -x local examples/test.pig


You should find the results in the 'test' DB inside the 'pig.output' collection.

TODO
----------
- Use sharding chunks as splits when sharded
  * Read from slaves, again for parallelized inputting?
- Pick reasonable split points for non-sharded collections
  * For initial release, no splits for non-sharded collections
- [Elastic map/reduce support?](http://aws.amazon.com/elasticmapreduce/faqs)
- Support for "Merge" Jobs (e.g. combine results of multiple map/reduces esp. from diff. inputs in a single join job - this is supported by Hadoop w/ implementation of special interfaces)
- Support Streaming for Python/Ruby implementation
- [Flume Sink](http://www.cloudera.com/blog/2010/07/whats-new-in-cdh3b2-flume/) asked for by several people
- Full support for appropriate 'alternate' Hadoop Interfaces
  * We already support Pig for Output (get input working)
  * [Cascading](http://www.cascading.org/) Seems to be popular as well and should be evaluated


KNOWN ISSUES
--------------

You cannot configure bare regexes (e.g. /^foo/) in the config xml as they won't parse.  
Use {"$regex": "^foo", "$options": ""} instead. .. Make sure to omit the slashes.

We need a BSONWritable implementation

Running Streaming:
  hadoop jar $HADOOP_STREAMING -conf examples/treasury_yield/resources/mongo-treasury_yield.xml -libjars mongo-hadoop.jar,lib/mongo-java-driver-2.3.jar  -mapper examples/treasury_yield/src/mapper.py -reducer examples/treasury_yield/src/reducer.py -inputformat com.mongodb.hadoop.mapred.MongoInputFormat -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat -input README.md -output foo.md
