Building Hadoop Streaming Support
=================================

As noted in the "Building the Adapter" guide, not every version of Hadoop supports the features required to use MongoDB with Streaming. Please consult those build instructions to ensure your version of Hadoop is supported for MongoDB + Streaming.

Due to the manner Hadoop Streaming is used, *mongo-hadoop-streaming* requires the construction of a "fat" (sometimes known as a "shaded" or "assembly") jar, which contains all of its dependencies including the Java Driver. To this end, simply using the `package` task in *sbt* is not enough. To build a proper package, the following *sbt* command is used:

    ./sbt mongo-hadoop-streaming/assembly

This will create a new "fat" jar in: 

    streaming/target/mongo-hadoop-streaming-assembly-$version$.jar

This jar file is runnable with `hadoop jar`, and contains all of the dependencies necessary to run the job. 

### Setting up Language Support

Each individual scripting language will have different requirements for working with MongoDB + Hadoop Streaming.  Once you have the jar file built for *mongo-hadoop-streaming*, you will need to build and deploy the support libraries for your chosen language.

It will also be necessary to ensure these libraries are available on each Hadoop node in your cluster, along with the *mongo-hadoop-core* driver as outlined in the main setup instructions.  However, you do not need to distribute the *mongo-hadoop-streaming* jar anywhere.

#### Python Streaming Setup

Working with Python streaming support will require two Python modules installed on each cluster node:

* pymongo 2.0+ 
* PyMongo_Hadoop

*pymongo* is available via the normal installation/distribution methods for Python modules.  PyMongo_Hadoop is distributed with this source, and you can build and distribute it from there.

##### Building PyMongo_Hadoop

You will find the source for the *pymongo_hadoop* Python module under `streaming/language_support/python`. To create a package to distribute to each of your Hadoop nodes, the easiest process is:

    python setup.py bdist_egg

After which you will find an egg file underneath the `dist` directory.  You may then install this egg file to each of your Hadoop nodes.  If you are working on a single development node, you may find it simpler to run the `develop` task on setup.py rather than installing an egg file.

The following Python script may prove useful in validating your installation on each node:

```python
#!/usr/bin/env python

try:
    import pymongo
    from bson import _elements_to_dict, InvalidBSON
except:
    raise Exception("Cannot find a valid pymongo installation.")

try:
    from pymongo_hadoop import BSONInput
except:
    raise Exception("Cannot find a valid pymongo_hadoop installation")

print "*** Everything looks OK. All required modules were found."

``` 
