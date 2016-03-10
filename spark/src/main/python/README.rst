``pymongo-spark`` integrates `PyMongo`_, the Python driver for MongoDB, with
`PySpark`_, the Python front-end for `Apache Spark`_. It is designed to be used
in tandem with ``mongo-hadoop-spark.jar``, which can be found under
``spark/build/libs`` after compiling mongo-hadoop (the jar will be available for
download when 1.5 is officially released). For instructions on how to compile
mongo-hadoop, please see the main project's README.

.. _PyMongo: https://pypi.python.org/pypi/pymongo
.. _PySpark: http://spark.apache.org/docs/latest/api/python/pyspark.html
.. _Apache Spark: https://spark.apache.org
.. _releases page: https://github.com/mongodb/mongo-hadoop/releases
.. _mongo-hadoop: https://github.com/mongodb/mongo-hadoop

Installation
------------

1. Download `mongo-hadoop`_::

     git clone https://github.com/mongodb/mongo-hadoop.git

2. Go to the pymongo-spark directory of the project and install::

     cd mongo-hadoop/spark/src/main/python
     python setup.py install

You'll also need to put ``mongo-hadoop-spark.jar`` (see above for instructions
on how to obtain this) somewhere on Spark's ``CLASSPATH`` prior to using this
package.

Usage
-----

``pymongo-spark`` works by monkey-patching PySpark's RDD and SparkContext
classes. All you need to do is call the ``activate()`` function in
``pymongo-spark``::

   import pymongo_spark
   pymongo_spark.activate()

   # You are now ready to use BSON and MongoDB with PySpark.

**Make sure to set the appropriate options to put mongo-hadoop-spark.jar on
Spark's CLASSPATH**. For example::

   bin/pyspark --jars mongo-hadoop-spark.jar \
               --driver-class-path mongo-hadoop-spark.jar

You might also need to add ``pymongo-spark`` and/or ``PyMongo`` to Spark's
``PYTHONPATH`` explicitly::

   bin/pyspark --py-files /path/to/pymongo_spark.py,/path/to/pymongo.egg

Examples
--------

Read from MongoDB
.................

::

   >>> mongo_rdd = sc.mongoRDD('mongodb://localhost:27017/db.collection')
   >>> print(mongo_rdd.first())
   {u'_id': ObjectId('55cd069c6e32abacca39da2b'),
    u'hello': u'from MongoDB!'}

Write to MongoDB
................

::

   >>> some_rdd.saveToMongoDB('mongodb://localhost:27017/db.output_collection')

Reading from a BSON File
........................

::

   >>> file_path = 'my_bson_files/dump.bson'
   >>> rdd = sc.BSONFileRDD(file_path)
   >>> rdd.first()
   {u'_id': ObjectId('55cd071e6e32abacca39da2c'),
    u'hello': u'from BSON!'}

Write to a BSON File
....................

::

   >>> some_rdd.saveToBSON('my_bson_files/output')
