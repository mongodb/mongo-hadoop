STREAMING
----------

Streaming support + MongoDB **requires** your Hadoop distribution include the patches for the following issues:

* [HADOOP-1722 - Make streaming to handle non-utf8 byte array](https://issues.apache.org/jira/browse/HADOOP-1722)
* [HADOOP-5450 - Add support for application-specific typecodes to typed bytes](https://issues.apache.org/jira/browse/HADOOP-5450)
* [MAPREDUCE-764 - TypedBytesInput's readRaw() does not preserve custom type codes](https://issues.apache.org/jira/browse/MAPREDUCE-764)

For the mainline Apache Hadoop distribution, these patches were merged for the 0.21.0 release.  We have verified as well that the [Cloudera](http://cloudera.com) distribution (while based on 0.20.x still) includes these patches in CDH3 Update 1; anecdotal evidence (which needs confirmation) indicates they may have been there since CDH2, and likely exist in CDH3 as well.

Building Streaming
------------------

`./sbt mongo-hadoop-streaming/assembly`

This will create a new “fat” jar in: `streaming/target/mongo-hadoop-streaming-assembly-1.0.0.jar`

This jar file is runnable with hadoop jar, and contains all of the dependencies necessary to run the job.

Setting up Language Support

Each individual scripting language will have different requirements for working with MongoDB + Hadoop Streaming. Once you have the jar file built for mongo-hadoop-streaming, you will need to build and deploy the support libraries for your chosen language.

It will also be necessary to ensure these libraries are available on each Hadoop node in your cluster, along with the mongo-hadoop-core driver as outlined in the main setup instructions. However, you do not need to distribute the mongo-hadoop-streaming jar anywhere.
