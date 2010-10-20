
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

Open Items
----------
- Support and document XML Job configurations


