MongoDB Flume Adapter
=======================

Provides integration for Flume and MongoDB, currently as a sink only (Data can be written to MongoDB)

You'll need to copy this directory into flume's source directory under 'plugins'.

Run `ant jar`, copy the `mongodb-flume-plugin.jar` into flume's lib, and add the following to `conf/flume-conf.xml`:

    <property>
      <name>flume.plugin.classes</name>
      <value>com.mongodb.flume.MongoDBSink</value>
      <description>Comma separated list of plugin classes</description>
    </property>
    

From there, you can configure MongoDB as a sink using our [Standard URI Format](http://www.mongodb.org/display/DOCS/Connections).

A simple test script, using the pigtutorial sample log:

    flume node_nowatch -l -s -n dump -c 'dump: text("/Users/bwmcadams/code/mongodb/mongo-hadoop/examples/pigtutorial/resources/excite-small.log") | mongoDBSink("mongodb://localhost/test.flume");'


This will dump the contents of the input file as logged events into the `test` databasse, `flume` collection.


  
