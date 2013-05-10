t#Mongo-Hadoop Adapter


##Purpose

The mongo-hadoop adapter is a library which allows MongoDB to be used as an input source, or output destination, for Hadoop MapReduce tasks. It is designed to allow greater flexibility, 

## Features

* Generate data splits from standalone, replica set, or sharded configurations
* Source data can be filtered with query
* Supports Hadoop Streaming, to allow job code to be written in any language (python, ruby, nodejs supported)
* Can read data from MongoDB backup files residing on s3, hdfs, or local filesystems
* Can write data out in .bson format, which can then be imported to any mongo database with `mongorestore`

## Download

List the binaries here.
Maven artifacts?

## Building

To build, first edit the value for `hadoopRelease in ThisBuild` in the build.sbt file to select the distribution of Hadoop that you want to build against. For example to build for CDH4:

    hadoopRelease in ThisBuild := "cdh4"                                                                                                                                                                                                       

or for Hadoop 1.0.x:

    hadoopRelease in ThisBuild := "1.0"                                                                                                                                                                                                       

To figure out which value you need to set in this file, refer to the list of distributions below.
Then run `./sbt package` to build the jars, which will be generated in the `target/` directory.

After successfully building, you must copy the jars to the lib directory on each node in your hadoop cluster. This is usually one of the following locations, depending on which Hadoop release you are using:

* `$HADOOP_HOME/lib/`
* `$HADOOP_HOME/share/hadoop/mapreduce/`
* `$HADOOP_HOME/share/hadoop/lib/`


## Supported Distributions of Hadoop

* ###Apache Hadoop 1.0
   Does **not** support Hadoop Streaming.

   Build using `"1.0"` or `"1.0.x"`

* ###Apache Hadoop 0.20.*
   Does **not** support Hadoop Streaming
   
   Includes Pig 0.9.2.
   
   Build using `"0.20"` or `"0.20.x"`
   
* ###Apache Hadoop 0.23
   Includes Pig 0.9.2.
   
   Includes support for Streaming
   
   Build using `"0.23"` or `"0.23.x"`

* ###Apache Hadoop 0.21
   Includes Pig 0.9.1
   
   Includes support for Streaming
   
   Build using `"0.21"` or `"0.21.x"`

* ###Cloudera Hadoop Release 3
    This is derived from Apache Hadoop 0.20.2 and includes custom patches.
    
   Includes support for streaming and Pig 0.8.1.

   Build with `"cdh3"`

* ###Cloudera Hadoop Release 4
   This is the newest release from Cloudera which is based on Apache Hadoop 0.23.
   
   Includes support for Streaming and Pig 0.9.2.
   
   Build with `"cdh4"`



## Configuration

`mongo.job.mapper`  - sets the class to be used as the implementation for `Mapper`

`mongo.job.reducer`  - sets the class to be used as the implementation for `Mapper`

`mongo.job.partitioner`  - sets the class to be used as the implementation for `Mapper`


`mongo.auth.uri` - specify an auxiliary [mongo URI](http://docs.mongodb.org/manual/reference/connection-string/) to authenticate against when constructing splits. If you are using a sharded cluster and your `config` database requires authentication, and its username/password differs from the collection that you are running Map/Reduce on, then you may need to use this option so that Hadoop can access collection metadata in order to compute splits. An example URI: `mongodb://username:password@cyberdyne:27017/config`

`mongo.input.uri` - specify an input collection (including auth and readPreference options) to use as the input data source for the Map/Reduce job. This takes the form of a 
[mongo URI](http://docs.mongodb.org/manual/reference/connection-string/), and by specifying options you can control how and where the input is read from.

Examples:

`mongodb://joe:12345@weyland-yutani:27017/analytics.users?readPreference=secondary` - Authenticate with "joe"/"12345" and read from only nodes in state SECONDARY from the collection "users" in database "analytics". 
`mongodb://sue:12345@weyland-yutani:27017/production.customers?readPreferenceTags=dc:tokyo,type:hadoop` - Authenticate with "joe"/"12345" and read the collection "users" in database "analytics" from only nodes tagged with "dc:tokyo" and "type:hadoop" from the collection "users" in database "analytics". 



## Streaming

### Python
### Ruby
### NodeJS

## Using Backup Files (.bson)

## Example 1

## Example 2

## Usage with Amazon Elastic MapReduce

## Usage with Pig

Pig 0.9 and earlier have issues with non-named tuples. You may need to unpack and name the tuples explicitly, for example: 

    The tuple `(1,2,3)` can not be stored correctly. But,

    `FLATTEN((1,2,3)) as v1, v2, v3` can successfully be stored as `{'v1': 1, 'v2': 2, 'v3': 3}`
Pig 0.10 and later handles them correctly.

## Usage with Hive

## Notes for Contributors

Please make sure your code passes all unit tests.
If your code introduces new features, please add tests that cover them if possible.


### Maintainers
Mike O'Brien (mikeo@10gen.com)

### Contributors
* Brendan McAdams brendan@10gen.com
* Eliot Horowitz erh@10gen.com
* Ryan Nitz ryan@10gen.com
* Russell Jurney (@rjurney) (Lots of significant Pig improvements)
* Sarthak Dudhara sarthak.83@gmail.com (BSONWritable comparable interface)
* Priya Manda priyakanth024@gmail.com (Test Harness Code)
* Rushin Shah rushin10@gmail.com (Test Harness Code)
* Joseph Shraibman jks@iname.com (Sharded Input Splits)
* Sumin Xia xiasumin1984@gmail.com (Sharded Input Splits)

### Support

Issue tracking: https://jira.mongodb.org/browse/HADOOP/

Discussion: http://groups.google.com/group/mongodb-user/

