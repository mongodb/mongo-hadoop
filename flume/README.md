MongoDB Flume Adapter
=======================

Provides integration for Flume and MongoDB, currently as a sink only (Data can be written to MongoDB)

You'll need to have Maven installed to build this. When you're ready to build, run 'mvn install'. This will build a JAR file to `~/.m2/repository/org/mongodb/mongo-flume/1.0-SNAPSHOT/mongo-flume-1.0-SNAPSHOT.jar`, which you will need to copy to Flume's lib directory. You'll also need to copy `~/.m2/repository/org/mongodb/mongo-java-driver/2.7.2/mongo-java-driver-2.7.2.jar` to the same directory. After this, you should add the following to `conf/flume-conf.xml`:

    <property>
      <name>flume.plugin.classes</name>
      <value>com.mongodb.flume.MongoDBSink</value>
      <description></description>
    </property>


From there, you can configure MongoDB as a sink using our [Standard URI Format](http://www.mongodb.org/display/DOCS/Connections).

To test this, you'll need to set up Flume in [Pseudo-distributed mode](http://archive.cloudera.com/cdh/3/flume/UserGuide/#_pseudo_distributed_mode). First, run a master node using:

    flume master

You can check that this is running by accessing the master node [configuration page](http://localhost:35871/). You should then start a slave node using:

    flume node_nowatch

Verify that this is running correctly by accessing the [admin page](http://localhost:35862/).

Go back to the master node [configuration page](http://localhost:35871/) and click on the [config link](http://localhost:35871/flumeconfig.jsp) to configure a node. In the dropdown labelled 'Configure node:' select the IP of your local host. For 'Source:', enter 'text("/Users/bwmcadams/code/mongodb/mongo-hadoop/examples/pigtutorial/resources/excite-small.log")'. For 'Sink:', enter 'mongoDBSink("mongodb://localhost/test.flume")'.
