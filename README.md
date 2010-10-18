
MongoDB Hadoop Adapter
=======================

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


