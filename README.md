# MongoDB Hadoop Adapter

CURRENT RELEASE: 1.0.0-rc0

This release primarly supports Hadoop 1.0 or Cloudera CHD3 Update 3
(Which ships 0.20.2).  If you wish to use Hadoop Streaming with
MongoDB, please see the notes on Streaming Hadoop versions below.

This product is only supports on MongoDB 2.0+; although it should
(mostly) work with 1.8.x. We cannot provide support for legacy MongoDB
builds.

**Note**: If you have questions please email the
[mongodb-user Mailing List](http://groups.google.com/group/mongodb-user),
rather than directly contacting contributors or maintainers.

## Maintainers

* Brendan McAdams <brendan@10gen.com>
* Eliot Horowitz <erh@10gen.com>
* Ryan Nitz

## Contributors

* Sarthak Dudhara <sarthak.83@gmail.com> (BSONWritable comparable interface)
* Priya Manda <priyakanth024@gmail.com> (Test Harness Code)
* Rushin Shah <rushin10@gmail.com> (Test Harness Code)
* Joseph Shraibman <jks@iname.com> (Sharded Input Splits)
* Sumin Xia <xiasumin1984@gmail.com> (Sharded Input Splits)

## Support

You will need the MongoDB Java Driver 2.7.3+.

Issue tracking: https://jira.mongodb.org/browse/HADOOP/

Discussion: http://groups.google.com/group/mongodb-user/

## Building the Adapter

The Mongo-Hadoop adapter uses the
[SBT Build Tool](https://github.com/harrah/xsbt) tool for
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

or:

    hadoopRelease in ThisBuild := "cloudera"

Configures a build against Cloudera CDH3u3, while:

    hadoopRelease in ThisBuild := "0.21"

Configures a build against Hadoop 0.21 from the mainline Apache distribution.

Unfortunately, we are not aware of any Maven repositories that contain
artifacts for Hadoop 0.21 at present. You may need to resolve these
dependencies by hand if you chose to build using this
configuration. We also publish releases to the central Maven
repository with artifacts customized using the dependent release
name. Our "default" build has no artifact name attached and supports
Hadoop 1.0.

After building, you will need to place the "core" jar and the
"mongo-java-driver" in the `lib` directory of each Hadoop server.

The MongoDB-Hadoop Adapter supports the following releases. Valid keys
for configuration and Maven artifacts appear below each release.

### Cloudera Release 3

This derives from Apache Hadoop 0.20.2, but includes many custom
patches. Patches include binary streaming, and Pig 0.8.1.  This
target compiles *ALL* Modules, including Streaming.

- cdh
- cdh3
- cloudera
- Maven artifact: "org.mongodb" / "mongo-hadoop_cdh3u3"

### Apache Hadoop 0.20.205.0

This includes Pig 0.9.1 and does *NOT* support Hadoop Streaming.

- 0.20
- 0.20.x
- Maven artifact: "org.mongodb" / "mongo-hadoop_0.20.205.0"

### Apache Hadoop 1.0.0

This includes Pig 0.9.1 and does *NOT* support Hadoop Streaming.

- 1.0
- 1.0.x
- Maven artifact: "org.mongodb" / "mongo-hadoop_1.0.0"

## Apache Hadoop 0.21.0

This includes Pig 0.9.1 and Hadoop Streaming.

- 0.21
- 0.21.x

This build is **not** published to Maven because of upstream
dependency availability.

### Apache Hadoop 0.23

Support is *forthcoming*.

This is an alpha branch with ongoing work by
[Hortonworks](http://hortonworks.com). Apache Hadoop 0.23 is "newer"
than Apache Hadoop 1.0.

The MongoDB Hadoop Adapter currently supports the following features.

## Hadoop MapReduce

Provides working *Input* and *Output* adapters for MongoDB.  You may
configure these adapters with XML or programatically. See the
WordCount examples for demonstrations of both approaches.  You can
specify a query, fields and sort specs in the XML config as JSON or
programatically as a DBObject.

### Splitting up MongoDB Source Data for the InputFormat

The MongoDB Hadoop Adapter makes it possible to create multiple
*InputSplits* on source data originating from MongoDB to
optimize/paralellize input processing for Mappers.

If '*mongo.input.split.create_input_splits*' is **false** (it defaults
to **true**) then MongoHadoop will use **no** splits. Hadoop will
treat the entire collection in a single, giant, *Input*.  This is
primarily intended for debugging purposes.

When true, as by default, the following possible behaviors exist:

  1. For unsharded the source collections, MongoHadoop follows the
     "unsharded split" path. (See below.)

  2. For sharded source collections:

     * If '*mongo.input.split.read_shard_chunks*' is **true**
       (defaults **true**) then we pull the chunk specs from the
       configuration server, and turn each shard chunk into an *Input
       Split*.  Basically, this means the mongodb sharding system does
       99% of the preconfig work for us and is a good thing™

     * If '*read_shard_chunks*' is **false** and
       '*mongo.input.split.read_from_shards*' is **true** (it defaults
       to **false**) then we connect to the `mongod` or replica set
       for each shard individually and each shard becomes an input
       split. The entire content of the collection on the shard is one
       split. Only use this configuration in rare situations.

     * If '*read_shard_chunks*' is **true** and
       '*mongo.input.split.read_from_shards*' is **true** (it defaults
       to **false**) MongoHadoop reads the chunk boundaries from
       the config server but then reads data directly from the shards
       without using the `mongos`.  While this may seem like a good
       idea, it can cause erratic behavior if MongoDB balances chunks
       during a Hadoop job. This is not a recommended configuration
       for write heavy applications but may provide effective
       parallelism in read heavy apps.

     * If both '*create_input_splits*' and '*read_from_shards*' are
       **false** disabled then we pretend there is no sharding and use
       the "unsharded split" path. When '*read_shard_chunks*' is
       **false** MongoHadoop reads everything through mongos as a
       single split.

### "Unsharded Splits"

"Unsharded Splits" refers to the system that MongoHadoop uses to
calculate new splits. You may use "Unsharded splits" with sharded
MongoDB options.

This is only used:

- for unsharded collections when
  '*mongo.input.split.create_input_splits*' is **true**.

- for sharded collections when
  '*mongo.input.split.create_input_splits*' is **true** *and*
  '*read_shard_chunks*' is **false**.

In these cases, MongoHadoop generates multiple InputSplits. Users
have control over two factors in this system.

* *mongo.input.split_size* - Controls the maximum number of megabytes
  of each split.  The current default is 8, based on assumptions
  prior experience with Hadoop. MongoDB's default of 64 megabytes
  may be a bit too large for most deployments.

* *mongo.input.split.split_key_pattern* - Is a MongoDB key pattern
  that follows [the same rules as shard key selection](http://www.mongodb.org/display/DOCS/Sharding+Introduction#ShardingIntroduction-ShardKeys).
  This key pattern has some requires, (i.e. must have an index,
  unique, and present in all documents.) MongoHadoop uses this key to
  determine split points. It defaults to `{ _id: 1 }` but you may find
  that its more ideal  to optimize the mapper distribution by
  configuring this value.

For all three paths, you may specify a custom query filter for the
input data. *mongo.input.query* represents a JSON document containing
a MongoDB query.  This will be properly combined with the index
filtering on input splits allowing you to MapReduce a subset of your
data but still get efficient splitting.

### Pig

MongoHadoop includes the MongoStorage module for Pig. Currently, this
only supports *saving* data to MongoDB. Support for load support is
forthcoming in a future release.

## Examples

### WordCount

There are two example WordCount processes for Hadoop MapReduce in `examples/wordcount`
Both read strings from MongoDB and save the count of word frequency.

These examples read documents in the `test` database, stored in the
collection named `in`. They will count the frequency defined in field
`x`.

The examples save results in db `test`, collection `out`.

`WordCount.java` is a programatically configured MapReduce job, where
all of the configuration params are setup in the Java code.  You can
run this with the ant task `wordcount`.

`WordCountXMLConfig.java` configures the MapReduce job using only XML
files, with JSON for queries.  See
`examples/wordcount/src/main/resources/mongo-wordcount.xml` for the
example configuration.  You can run this with the ant task
`wordcountXML`, or with a Hadoop command that resembles the following:

    hadoop jar core/target/mongo-hadoop-core-1.0.0-rc0.jar com.mongodb.hadoop.examples.WordCountXMLConfig -conf examples/wordcount/src/main/resources/mongo-wordcount.xml

You will need to copy the `mongo-java-driver.jar` file into your
Hadoop `lib` directory before this will work.

### Treasury Yield

The treasury yield example demonstrates working with a more complex
input BSON document and calculating an average.

It uses a database of daily US Treasury Bid Curves from 1990 to
Sept. 2010 and runs them through to calculate annual averages.

There is a JSON file `examples/treasury_yield/src/main/resources/yield_historical_in.json`
which you should import into the `yield_historical.in` collection in
the `demo` db.

You may import the sample data into the `mongos` host by issuing the
following command:

    mongoimport --db demo --collection yield_historical.in --type json --file examples/treasury_yield/src/main/resources/yield_historical_in.json

This command assumes that `mongos` is running on the localhost
interface on port `27017`. You'll need to setup the mongo-hadoop and
mongo-java-driver jars in your Hadoop installations "lib"
directory. After importing the data, run the test with the following
command on the Hadoop master:

    hadoop jar core/target/mongo-hadoop-core-1.0.0-rc0.jar com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig -conf examples/treasury_yield/src/main/resources/mongo-treasury_yield.xml

To confirm the test ran successfully, look at the `demo` database and
query the `yield_historical.out collection`.

### Pig

The MongoHadoop distribution includes a modified version of the Pig
Tutorial from the Pig distribution for testing.

This script differs from the pig tutorial in that it saves the job
results to MongoDB.

The use of Pig assumes you have Hadoop and Pig installed and
configured on your system.

Make sure you've built using `ant jar` and then run:

    pig -x local examples/test.pig

You should find the results in the `test` DB inside the `pig.output`
collection.


## KNOWN ISSUES

### Open Issues

* You cannot configure bare regexes (e.g. /^foo/) in the config xml as
  they won't parse. Use {"$regex": "^foo", "$options": ""}
  instead. .. Make sure to omit the slashes.

* [HADOOP-19 - MongoStorage fails when tuples w/i bags are not named](https://jira.mongodb.org/browse/HADOOP-19)

  This is due to an open Apache bug, [PIG-2509](https://issues.apache.org/jira/browse/PIG-2509).

### Streaming

Streaming support in MongoHadoop **requires** that the Hadoop
distribution include the patches for the following issues:

* [HADOOP-1722 - Make streaming to handle non-utf8 byte array](https://issues.apache.org/jira/browse/HADOOP-1722)
* [HADOOP-5450 - Add support for application-specific typecodes to typed bytes](https://issues.apache.org/jira/browse/HADOOP-5450)
* [MAPREDUCE-764 - TypedBytesInput's readRaw() does not preserve custom type codes](https://issues.apache.org/jira/browse/MAPREDUCE-764)

The mainline Apache Hadoop distribution merged these patches for the
0.21.0 release. We have verified as well that the
[Cloudera](http://cloudera.com) distribution (while based on 0.20.x
still) includes these patches in CDH3 Update 1+ (We build against
Update 3 now); anecdotal evidence (which needs confirmation) indicates
they may have been there since CDH2, and likely exist in CDH3 as well.


By default, The Mongo-Hadoop project builds against Apache 0.20.203 which does *not* include these patches.  To build/enable Streaming support you must build against either Cloudera CDH3u1 or Hadoop 0.21.0.

Additionally, note that Hadoop 1.0 is based on the 0.20 release.  As such, it *does not include* the patches necessary for streaming.  This is frustrating and upsetting but unfortunately out of our hands.  We are working on attempting to get these patches backported into a future release or finding an additional workaround.
