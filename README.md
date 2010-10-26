
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
- Specify a query in config as json
- Specify a projection in config as json
- Specify limit & skip in config
- Specify sort as json
- Set slave ok by default?
- Use sharding chunks as splits when sharded
  * Read from slaves, again for parallelized inputting?
  * See how I did luau, with threads spun up to suck in multiple inputs at once (IIRC)
- Pick reasonable split points for non-sharded collections
- Support and document XML Job configurations
- [Elastic map/reduce support?](http://aws.amazon.com/elasticmapreduce/faqs)
- Support for "Merge" Jobs (e.g. combine results of multiple map/reduces esp. from diff. inputs in a single join job - this is supported by Hadoop w/ implementation of special interfaces)
- Support Streaming for Python/Ruby implementation
- Full support for appropriate 'alternate' Hadoop Interfaces
  * We already support Pig for Output (get input working)
  * [Cascading](http://www.cascading.org/) Seems to be popular as well and should be evaluated
- Do we need support for Compression? (e.g. how compressed is BSON by default versus what we can achieve on a compression<->performance ratio balance with something like LZO)
- Add a sourcecode filter which detects usages of the deprecated api namespace (org.apache.hadoop.mapred) and errors out for safety reasons

KNOWN ISSUES
--------------

You cannot configure bare regexes (e.g. /^foo/) in the config xml as they won't parse.  Use {"$regex": "^foo", "$options": ""} instead. .. Make sure to omit the slashes
