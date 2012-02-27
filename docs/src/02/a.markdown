Building the Adapter
===================

The Mongo-Hadoop adapter uses the
[SBT Build Tool][xsbt] tool for
compilation. SBT provides superior support for discreet configurations
targeting multiple Hadoop versions. The distribution includes
self-bootstrapping copy of SBT in the distribution as `sbt`.  Create a
copy of the jar files using the following command:

    ./sbt package

The MongoDB Hadoop Adapter supports a number of Hadoop releases. You
can change the Hadoop version supported by the build by modifying the
value of `hadoopRelease` in the `build.sbt` file. For instance, set
this value to:

    hadoopRelease in ThisBuild := "cdh3"

Configures a build against Cloudera CDH3u3, while:

    hadoopRelease in ThisBuild := "1.0"

Configures a build against Hadoop 1.0 from the mainline Apache distribution.

We also publish releases to the central Maven
repository with artifacts customized using the dependent release
name. Our "default" build has no artifact name attached and supports
Hadoop 1.0.

After building, you will need to place the *mongo-hadoop-core* jar and the
*mongo-java-driver* in the `lib` directory of each Hadoop server.

##### Streaming Support

Certain builds of Hadoop do not support features necessary for *mongo-hadoop* Streaming, and building with those versions will not produce an artifact for *mongo-hadoop-streaming*.

The MongoDB-Hadoop Adapter supports the following releases. Valid keys
for configuration and Maven artifacts appear below each release.


#### Apache Hadoop 1.0.0

This includes Pig $mainlinePig$ and does *NOT* support Hadoop Streaming.

- 1.0
- 1.0.x
- Maven artifact: "org.mongodb" / "mongo-hadoop_1.0.0"

#### Apache Hadoop 0.20.205.0

This includes Pig $mainlinePig$ and does *NOT* support Hadoop Streaming.

- 0.20
- 0.20.x
- Maven artifact: "org.mongodb" / "mongo-hadoop_0.20.205.0"


#### Cloudera Release 3

This derives from Apache Hadoop 0.20.2, but includes many custom
patches. Patches include binary streaming, and Pig $cdh3Pig$.  This
target compiles *ALL* Modules, including Streaming.

- cdh3
- Maven artifact: "org.mongodb" / "mongo-hadoop_cdh3u3"


#### Apache Hadoop 0.23

This is an alpha branch of Hadoop; despite the misleading version numbers, Apache Hadoop 0.23 is "newer" than Apache Hadoop 1.0. Hadoop 0.23 is also the basis for Cloudera's CDH4 Beta. This target compiles *ALL* modules, including Streaming and Pig $mainlinePig$.  Note however that we *do not* support the next-generation [YARN][hadoop-yarn] at this time; support is planned for *mongo-hadoop* v1.1.

- 0.23
- 0.23.x
- Maven Artifact: "org.mongodb" / "mongo-hadoop_0.23.0" 

#### Cloudera Release 4 (Beta 1) 

This is the latest beta of Cloudera's distribution, based upon the 0.23 alpha branch of Hadoop; despite the misleading version numbers, Apache Hadoop 0.23 is "newer" than Apache Hadoop 1.0. This target compiles *ALL* modules, including Streaming and Pig $mainlinePig$.Note however that we *do not* support the next-generation [YARN][hadoop-yarn] at this time; support is planned for *mongo-hadoop* v1.1.


- cdh4
- Maven Artifact: "org.mongodb" / "mongo-hadoop_cdh4b1" 

#### Apache Hadoop 0.21.0

This includes Pig 0.9.1 and Hadoop Streaming.

- 0.21
- 0.21.x

This build is **not** published to Maven because of upstream
dependency availability. 

Unfortunately, we are not aware of any Maven repositories that contain
artifacts for Hadoop 0.21 at present. You may need to resolve these
dependencies by hand if you chose to build using this
configuration. 

[xsbt]: https://github.com/harrah/xsbt "SBT Build Tool"
[hadoop-yarn]: http://hadoop.apache.org/common/docs/r0.23.0/hadoop-yarn/hadoop-yarn-site/YARN.html
