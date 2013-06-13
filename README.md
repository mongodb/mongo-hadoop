#Mongo-Hadoop Adapter

##Purpose

The mongo-hadoop adapter is a library which allows MongoDB (or backup files in its data format, BSON) to be used as an input source, or output destination, for Hadoop MapReduce tasks. It is designed to allow greater flexibility and performance and make it easy to integrate data in MongoDB with other parts of the Hadoop ecosystem. 

Current release: **1.1**

## Features

* Can create data splits to read from standalone, replica set, or sharded configurations
* Source data can be filtered with queries using the MongoDB query language
* Supports Hadoop Streaming, to allow job code to be written in any language (python, ruby, nodejs currently supported)
* Can read data from MongoDB backup files residing on s3, hdfs, or local filesystems
* Can write data out in .bson format, which can then be imported to any mongo database with `mongorestore`
* Work with BSON/MongoDB documents in other Hadoop tools such as **Pig** and **Hive**.

## Download

* TODO List links to binaries here.
* TODO List maven artifacts

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
 
   This is the newest release from Cloudera which is based on Apache Hadoop 2.0. The newer MR2/YARN APIs are not yet supported, but MR1 is still fully compatible.
   
   Includes support for Streaming and Pig 0.11.1.
   
   Build with `"cdh4"`

## Configuration

[Configuration](CONFIG.md)

## Streaming

[Streaming](streaming/README.md)

## Using Backup Files (.bson)

Static .bson files (which is the format produced by the [mongodump](http://docs.mongodb.org/manual/reference/program/mongodump/) tool for backups) can also be used as input to Hadoop jobs, or written to as output files.

###Using .bson files for input

#####Setup

To use a bson file as the input for a hadoop job, you must set `mongo.job.input.format` to `"com.mongodb.hadoop.BSONFileInputFormat"` or use `MongoConfigUtil.setInputFormat(com.mongodb.hadoop.BSONFileInputFormat.class)`.

Then set `mapred.input.dir` to indicate the location of the .bson input file(s). The value for this property may be:

* the path to a single file,
* the path to a directory (all files inside the directory will be treated as BSON input files), 
* located on the local file system (`file://...`), on Amazon S3 (`s3n://...`), on a Hadoop Filesystem (`hdfs://...`), or any other FS protocol your system supports,
* a comma delimited sequence of these paths.

#####Code

BSON objects loaded from a .bson file do not necessarily have a _id field, so there is no key supplied to the `Mapper` field. Because of this, you should use `NullWritable` or simply `Object` as your input key for the map phase, and ignore the key variable in your code. For example:

	public void map(NullWritable key, BSONObject val, final Context context){
	   // …
	}

#####Splitting .BSON files for parallelism

Because BSON contains headers and length information, a .bson file cannot be split at arbitrary offsets because it would create incomplete document fragements. Instead it must be split at the boundaries between documents. To facilitate this the mongo-hadoop adapter refers to a small metadata file which contains information about the offsets of documents within the file. This metadata file is stored in the same directory as the input file, with the same name but starting with a "." and ending with ".splits". If this metadata file already exists in when the job runs, the `.splits` file will be read and used to directly generate the list of splits. If the `.splits` file does not yet exist, it will be generated automatically so that it is available for subsequent runs. To disable generation of this file, set 

The default split size is determined from the default block size on the input file's filesystem, or 64 megabytes if this is not available. You can set lower and upper bounds for the split size by setting values (in bytes) for `mapred.min.split.size` and `mapred.max.split.size`.
The `.splits` file contains bson objects which list the start positions and lengths for portions of the file, not exceeding the split size, which can then be read directly into a `Mapper` task. 

However, for optimal performance, it's faster to build this file locally before uploading to S3 or HDFS if possible. You can do this by running the script  `tools/bson_splitter.py`. The default split size is 64 megabytes, but you can set any value you want for split size by changing the value for `SPLIT_SIZE` in the script source, and re-running it.

 
###Producing .bson files as output

By using `BSONFileOutputFormat` you can write the output data of a Hadoop job into a .bson file, which can then be fed into a subsequent job or loaded into a MongoDB instance using `mongorestore`.

#####Setup

To write the output of a job to a .bson file, set `mongo.job.output.format` to `"com.mongodb.hadoop.BSONFileOutputFormat"` or use `MongoConfigUtil.setOutputFormat(com.mongodb.hadoop.BSONFileOutputFormat.class)`

Then set `mapred.output.file` to be the location where the output .bson file should be written. This may be a path on the local filesystem, HDFS, S3, etc.
Only one output file will be written, regardless of the number of input files used.

#####Writing splits during output

If you intend to feed this output .bson file into subsequent jobs, you can generate the `.splits` file on the fly as it is written, by setting `bson.output.build_splits` to `true`. This will save time over building the `.splits` file on demand at the beginning of another job. By default, this setting is set to `false` and no `.splits` files will be written during output.

####Settings for BSON input/output

* `bson.split.read_splits` 		- When set to `true`, will attempt to read + calculate split points for each BSON file in the input. When set to `false`, will create just *one* split for each input file, consisting of the entire length of the file. Defaults to `true`.
* `mapred.min.split.size` - Set a lower bound on acceptable size for file splits (in bytes). Defaults to 1.
* `mapred.max.split.size` - Set an upper bound on acceptable size for file splits (in bytes). Defaults to LONG_MAX_VALUE.
* `bson.split.write_splits` - Automatically save any split information calculated for input files, by writing to corresponding `.splits` files. Defaults to `true`.
* `bson.output.build_splits` - Build a `.splits` file on the fly when constructing an output .BSON file. Defaults to `false`.
* `bson.pathfilter.class` - Set the class name for a `[PathFilter](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/PathFilter.html)` to filter which files to process when scanning directories for bson input files. Defaults to `null` (no additional filtering). You can set this value to `com.mongodb.hadoop.BSONPathFilter` to restrict input to only files ending with the ".bson" extension.

## Usage with Amazon Elastic MapReduce

Amazon Elastic MapReduce is a managed Hadoop framework that allows you to submit jobs to a cluster of customizable size and configuration, without needing to deal with provisioning nodes and installing software.

Using EMR with mongo-hadoop allows you to run MapReduce jobs against mongo backup files stored in S3.

Submitting jobs using mongo-hadoop to EMR simply requires that the bootstrap actions fetch the dependencies (mongoDB java driver, mongo-hadoop-core libs, etc.) and place them into the hadoop distributions `lib` folders.

For a full example (running the enron example on Elastic MapReduce) please see [here](examples/elastic-mapreduce/README.md).

## Usage with Pig

[Documentation on Pig with Mongo-Hadoop](pig/README.md).

For examples on using Pig with mongo-hadoop, also refer to the [examples section](examples/README.md).

## Notes for Contributors

If your code introduces new features, please add tests that cover them if possible and make sure that the existing test suite  still passes. If you're not sure how to write a test for a feature or have trouble with a test failure, please post on the google-groups with details and we will try to help. 

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

