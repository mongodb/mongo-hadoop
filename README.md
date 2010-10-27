
MongoDB Hadoop Adapter
=======================

State of the Adapter
---------
This is currently under development and is not feature complete.

Issue tracking: https://github.com/mongodb/mongo-hadoop/issues

Discussion: http://groups.google.com/group/mongodb-user/

Examples
----------
src/examples/WordCount.java

Testing Pig
-----------

This assumes you have Hadoop & Pig installed and setup on your machine...

Copy the sample input file using `hadoop fs -copyFromLocal src/examples/excite-small.log excite-small.log`.

Make sure you've built using `ant jar` and then run:


    pig -x local src/examples/test.pig


You should find the results in the 'test' DB inside the 'pig.output' collection.

TODO
----------
- Set slave ok by default?
- Use sharding chunks as splits when sharded
  * Read from slaves, again for parallelized inputting?
- Pick reasonable split points for non-sharded collections
  * For initial release, no splits for non-sharded collections
- [Elastic map/reduce support?](http://aws.amazon.com/elasticmapreduce/faqs)
- Support for "Merge" Jobs (e.g. combine results of multiple map/reduces esp. from diff. inputs in a single join job - this is supported by Hadoop w/ implementation of special interfaces)
- Support Streaming for Python/Ruby implementation
- [Flume Sink|http://www.cloudera.com/blog/2010/07/whats-new-in-cdh3b2-flume/] asked for by several people
- Full support for appropriate 'alternate' Hadoop Interfaces
  * We already support Pig for Output (get input working)
  * [Cascading](http://www.cascading.org/) Seems to be popular as well and should be evaluated
- ** We treat '\_id', our main split segment, as an Object.  However this won't work well for custom types.  We should investigate allowing registration of custom BSONEncoder/Decoder which can be setup on the remote mapper **

KNOWN ISSUES
--------------

You cannot configure bare regexes (e.g. /^foo/) in the config xml as they won't parse.  Use {"$regex": "^foo", "$options": ""} instead. .. Make sure to omit the slashes

