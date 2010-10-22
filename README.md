
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
- Pick reasonable split points for non-sharded collections
- Support and document XML Job configurations
- [Elastic map/reduce support?](http://aws.amazon.com/elasticmapreduce/faqs)

