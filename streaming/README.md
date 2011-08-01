STREAMING
----------

Streaming support + MongoDB **requires** your Hadoop distribution include the patches for the following issues:

    * [HADOOP-1722 - Make streaming to handle non-utf8 byte array](https://issues.apache.org/jira/browse/HADOOP-1722)
    * [HADOOP-5450 - Add support for application-specific typecodes to typed bytes](https://issues.apache.org/jira/browse/HADOOP-5450)
    * [MAPREDUCE-764 - TypedBytesInput's readRaw() does not preserve custom type codes](https://issues.apache.org/jira/browse/MAPREDUCE-764)

For the mainline Apache Hadoop distribution, these patches were merged for the 0.21.0 release.  We have verified as well that the [Cloudera](http://cloudera.com) distribution (while based on 0.20.x still) includes these patches in CDH3 Update 1; anecdotal evidence (which needs confirmation) indicates they may have been there since CDH2, and likely exist in CDH3 as well.


By default, The Mongo-Hadoop project builds against Apache 0.20.203 which does *not* include these patches.  To build/enable Streaming support you must build against either Cloudera CDH3u1 or Hadoop 0.21.0; you can change the Hadoop version of the build in Maven by specifying the `hadoop.release` property:

        mvn -Dhadoop.release=cdh3 
        mvn -Dhadoop.release=cloudera

Will both build against Cloudera CDH3u1, while:


        mvn -Dhadoop.release=apache-hadoop-0.21


Will build against Hadoop 0.21 from the mainline Apache distribution.  Unfortunately we are not aware of any Maven Repositories which currently contain artifacts for Hadoop 0.21, and you may need to resolve these dependencies by hand if you choose to go down the 'Vanilla' route.
