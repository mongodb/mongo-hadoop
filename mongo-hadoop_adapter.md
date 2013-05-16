#Mongo-Hadoop Adapter


##Purpose

The mongo-hadoop adapter is a library which allows MongoDB (or backup files in its data format, BSON) to be used as an input source, or output destination, for Hadoop MapReduce tasks. It is designed to allow greater flexibility and performance and make it easy to integrate data in MongoDB with other parts of the Hadoop ecosystem. 

## Features

* Can create data splits to read from standalone, replica set, or sharded configurations
* Source data can be filtered with queries using the MongoDB query language
* Supports Hadoop Streaming, to allow job code to be written in any language (python, ruby, nodejs currently supported)
* Can read data from MongoDB backup files residing on s3, hdfs, or local filesystems
* Can write data out in .bson format, which can then be imported to any mongo database with `mongorestore`

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
   This is the newest release from Cloudera which is based on Apache Hadoop 0.23.
   
   Includes support for Streaming and Pig 0.9.2.
   
   Build with `"cdh4"`



## Configuration

######`mongo.job.mapper`  
 sets the class to be used as the implementation for `Mapper`

######`mongo.job.reducer` 
 sets the class to be used as the implementation for `Reducer`

######`mongo.job.combiner`  
 sets the class to be used for a combiner. Must implement `Reducer`.

######`mongo.job.partitioner`  
 sets the class to be used as the implementation for `Partitioner`

######`mongo.auth.uri` 
 specify an auxiliary [mongo URI](http://docs.mongodb.org/manual/reference/connection-string/) to authenticate against when constructing splits. If you are using a sharded cluster and your `config` database requires authentication, and its username/password differs from the collection that you are running Map/Reduce on, then you may need to use this option so that Hadoop can access collection metadata in order to compute splits. An example URI: `mongodb://username:password@cyberdyne:27017/config`

######`mongo.input.uri` 
specify an input collection (including auth and readPreference options) to use as the input data source for the Map/Reduce job. This takes the form of a 
[mongo URI](http://docs.mongodb.org/manual/reference/connection-string/), and by specifying options you can control how and where the input is read from.

**Examples**:

`mongodb://joe:12345@weyland-yutani:27017/analytics.users?readPreference=secondary` - Authenticate with "joe"/"12345" and read from only nodes in state SECONDARY from the collection "users" in database "analytics". 
`mongodb://sue:12345@weyland-yutani:27017/production.customers?readPreferenceTags=dc:tokyo,type:hadoop` - Authenticate with "joe"/"12345" and read the collection "users" in database "analytics" from only nodes tagged with "dc:tokyo" and "type:hadoop" from the collection "users" in database "analytics". 

######`mongo.input.query` 
 filter the input collection with a query. This query must be represented in JSON format, and use the [MongoDB extended JSON format](http://docs.mongodb.org/manual/reference/mongodb-extended-json/) to represent non-native JSON data types like ObjectIds, Dates, etc.

**Example**

    'mongo.input.query={"_id":{"$gt":{"$date":1182470400000}}}'
    
    //this is equivalent to {_id : {$gt : ISODate("2007-06-21T20:00:00-0400")}} in the mongo shell

######`mongo.input.split.use_range_queries`

This setting causes data to be read from mongo by adding `$gte/$lt` clauses to the query for limiting the boundaries of each split, instead of the default behavior of limiting with `$min/$max`. This may be useful in cases when using `mongo.input.query` is being used to filter mapper input, but the index that would be used by `$min/$max` is less selective than the query and indexes. Setting this to `'true'` lets the MongoDB server select the best index possible and still. This defaults to '`false'` if not set.

**Limitations** 

This setting should only be used when:

* The query specified in `mongo.input.query` does not already have filters applied against the split field. Will log an error otherwise.
* The same data type is used for the split field in all documents, otherwise you may get incomplete results.
* The split field contains simple scalar values, and is not a compound key (e.g., `"foo"` is acceptable but `{"k":"foo", "v":"bar"}` is not)
  
  
######`mongo.input.split.create_input_splits`

Defaults to `'true'`. When set, attempts to create multiple input splits from the input collection, for parallelism.
When set to `'false'` the entire collection will be treated as a **single** input split.
 
  
######`mongo.input.split.allow_read_from_secondaries`  
**Deprecated.**

Sets `slaveOk` on all outbound connections when reading data from splits. This is deprecated - if you would like to read from secondaries, just specify the appropriate [readPreference](http://docs.mongodb.org/manual/reference/connection-string/#read-preference-options) as part of your `mongo.input.uri` setting.

######`mongo.input.split.read_from_shards`

When reading data for splits, query the shard directly rather than via the `mongos`. This may produce inaccurate results if there are aborted/incomplete chunk migrations in process during execution of the job - to ensure correctness when using this setting, disable the balancer while running the job and also set `mongo.input.split.read_shard_chunks` to `'true'`.

######`mongo.input.split.split_key_pattern`

If you want to customize that split point for efficiency reasons (such as different distribution) on an un-sharded collection, you may set this to any valid field name. The restriction on this key name are the *exact same rules* as when sharding an existing MongoDB Collection.  You must have an index on the field, and follow the other rules outlined in the docs. Only usable when `mongo

 
## Streaming

### Overview

For distributions of Hadoop which support streaming, you can also use MongoDB collections as the input or output for these jobs as well. Here is a description of arguments needed to run a Hadoop streaming job including MongoDB support.
* Launch the job with `$HADOOP/bin/hadoop jar <location of streaming jar> …` 
* Depending on which hadoop release you use, the jar needed for streaming may be located in a different directory, commonly it is located somewhere like `$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar` 
* Provide dependencies for the job by either placing each jar file in one your hadoop's classpath directories, or explicitly list it on the command line using `-libjars <jar file>`. You will need to do this for the Java MongoDB driver as well as the Mongo-Hadoop core library.
* When using a mongoDB collection as input, add the arguments `-jobconf mongo.input.uri=<input mongo URI>` and `-inputformat com.mongodb.hadoop.mapred.MongoInputFormat`
* When using a mongoDB collection as output, add the arguments `-jobconf mongo.output.uri=<input mongo URI>` and `-outputformat com.mongodb.hadoop.mapred.MongoOutputFormat`
* When using BSON as input, use `-inputformat com.mongodb.hadoop.mapred.BSONFileInputFormat`.
* Specify locations for the `-input` and `-output` arguments. Even when using mongoDB for input or output, these are required; you can use temporary directories for these in such a case.
* Always add the arguments `-jobconf stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver` and `-io mongodb` so that decoders/encoders needed by streaming can be resolved at run time.
* Specify a `-mapper <mapper script>` and `-reducer <reducer script>`.
* Any other arguments needed, by using `-jobconf`. for example `mongo.input.query` or other options for controlling splitting behavior or filtering.

Here is a full example of a streaming command broken into separate lines for readability:

     $HADOOP_HOME/bin/hadoop jar 
         $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming* 
         -libjars /Users/mike/projects/mongo-hadoop/streaming/target/mongo-hadoop-streaming.jar 
         -input /tmp/in 
         -output /tmp/out 
         -inputformat com.mongodb.hadoop.mapred.MongoInputFormat 
         -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat 
         -io mongodb 
         -jobconf mongo.input.uri=mongodb://127.0.0.1:4000/mongo_hadoop.yield_historical.in?readPreference=primary
         -jobconf mongo.output.uri=mongodb://127.0.0.1:4000/mongo_hadoop.yield_historical.out 
         -jobconf stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver 
         -mapper /Users/mike/projects/mongo-hadoop/streaming/examples/treasury/mapper.py 
         -reducer /Users/mike/projects/mongo-hadoop/streaming/examples/treasury/reducer.py 
         -jobconf mongo.input.query={_id:{\\$gt:{\\$date:883440000000}}}

Also, refer to the `TestStreaming` class in the test suite for more concrete examples.

**Important note**: if you need to use `print` or any other kind of text output when debugging streaming Map/Reduce scripts, be sure that you are writing the debug statements to `stderr` or some kind of log file. Using `stdin` or `stdout` for any purpose other than communicating with the Hadoop Streaming layer will interfere with the encoding and decoding of data.

### Python

#####Setup

To use Python for streaming first install the python package `pymongo_hadoop` using pip or easy_install (For best performance, ensure that you are using the C extensions for BSON).

#####Mapper
To implement a mapper, write a function which accepts an iterable sequence of documents and calls `yield` to produce each output document, then call BSONMapper() against that function.
For example:

    from pymongo_hadoop import BSONMapper
    def mapper(documents):
        for doc in documents:
            yield {'_id': doc['_id'].year, 'bc10Year': doc['bc10Year']}

    BSONMapper(mapper)

#####Reducer
To implement a reducer, write a function which accepts two arguments: a key and an iterable sequence of documents matching that key. Compute your reduce output and pass it back to Hadoop with `return`. Then call `BSONReducer()` against this function. For example,

    from pymongo_hadoop import BSONReducer

    def reducer(key, values):
        _count = _sum = 0
        for v in values:
            _count += 1
            _sum += v['bc10Year']
        return {'_id': key, 'avg': _sum / _count,
                'count': _count, 'sum': _sum }

    BSONReducer(reducer)


### Ruby

* TODO

### NodeJS

#####Setup

Install the nodejs mongo-hadoop lib with `npm install node_mongo_hadoop`.

#####Mapper

Write a function that accepts two arguments: the input document, and a callback function. Call the callback function with the output of your map function. Then pass that function as an argument to node_mongo_hadoop.MapBSONStream. For example:

    function mapFunc(doc, callback){
        if(doc.headers && doc.headers.From && doc.headers.To){
            var from_field = doc['headers']['From']
            var to_field = doc['headers']['To']
            var recips = []
            to_field.split(',').forEach(function(to){
              callback( {'_id': {'f':from_field, 't':trimString(to)}, 'count': 1} )
            });
        }
    }
    node_mongo_hadoop.MapBSONStream(mapFunc);

#####Reducer

Write a function that accepts three arguments: the key from the map phase, an array of documents for that key, and a callback.
Call the callback function with the output result of reduce.

    var node_mongo_hadoop = require('node_mongo_hadoop')

    function reduceFunc(key, values, callback){
        var count = 0;
        values.forEach(function(v){
            count += v.count
        });
        callback( {'_id':key, 'count':count } );
    }

    node_mongo_hadoop.ReduceBSONStream(reduceFunc);


## Using Backup Files (.bson)

Static .bson files (which is the format produced by the [mongodump](http://docs.mongodb.org/manual/reference/program/mongodump/) tool for backups) can also be used as input to Hadoop jobs, or written to as output files.
Because BSON contains headers and length information, a .bson file cannot be split arbitrarily for the purposes of parallelization, it must be split along the correct boundaries between documents. To facilitate this the mongo-hadoop adapter uses a secondary metadata file which contains information about the offsets of documents within the file. This file should be created before running a job against a .bson file - to create it, run the script `bson-splitter.py <filename>`. If the file resides on S3 or HDFS, the `.splits` will be calculated and the file will be built and saved automatically. However, for optimal performance, it's faster to build this file locally before uploading to S3 or HDFS if possible. 

## Example 1

## Example 2

## Usage with Amazon Elastic MapReduce

Amazon Elastic MapReduce is a managed Hadoop framework that allows you to submit jobs to a cluster of customizable size and configuration, without needing to deal with provisioning nodes and installing software.

## Usage with Pig

### Reading into Pig

Pig 0.9 and earlier have issues with non-named tuples. You may need to unpack and name the tuples explicitly, for example: 

    The tuple `(1,2,3)` can not be stored correctly. But,

    `FLATTEN((1,2,3)) as v1, v2, v3` can successfully be stored as `{'v1': 1, 'v2': 2, 'v3': 3}`
Pig 0.10 and later handles them correctly.

### Reading into Pig

##### From a MongoDB collection

To load records from MongoDB database to use in a Pig script, a class called `MongoLoader` is provided. To use it, first register the dependency jars in your script and then specify the Mongo URI to load with the `MongoLoader` class.

    -- First, register jar dependencies
    REGISTER ../mongo-2.10.1.jar                    -- mongodb java driver  
    REGISTER ../core/target/mongo-hadoop-core.jar   -- mongo-hadoop core lib
    REGISTER ../pig/target/mongo-hadoop-pig.jar     -- mongo-hadoop pig lib

    raw = LOAD 'mongodb://localhost:27017/demo.yield_historical.in' using com.mongodb.hadoop.pig.MongoLoader;

`MongoLoader` can be used in two ways - schemaless mode and schema mode. By creating an instance of the class without specifying any field names in the constructor (as in the previous snippet) each record will appear to pig as a tuple containing a single  `Map` that corresponds to the document from the collection, for example: 
                  
                  ([bc2Year#7.87,bc3Year#7.9,bc1Month#,bc5Year#7.87,_id#631238400000,bc10Year#7.94,bc20Year#,bc7Year#7.98,bc6Month#7.89,bc3Month#7.83,dayOfWeek#TUESDAY,bc30Year#8,bc1Year#7.81])

However, by creating a MongoLoader instance with a specific list of field names, you can map fields in the document to fields in a Pig named tuple datatype. When used this way, `MongoLoader` takes two arguments:

`idAlias` - an alias to use for the `_id` field in documents retrieved from the collection. The string "_id" is not a legal pig variable name, so the contents of the field in `_id` will be mapped to a value in Pig accordingly by providing a value here. 

`schema` - a schema (list of fields/datatypes) that will map fields in the document to fields in the Pig records. See section below on Datatype Mapping for details.

Example:

    -- Load two fields from the documents in the collection specified by this URI
    -- map the "_id" field in the documents to the "id" field in pig
    > raw = LOAD 'mongodb://localhost:27017/demo.yield_historical.in' using com.mongodb.hadoop.pig.MongoLoader('id', 'id, bc10Year');
    > raw_limited = LIMIT raw 3;
    > dump raw_limited; 
    (631238400000,7.94)
    (631324800000,7.99)
    (631411200000,7.98)

##### From a .BSON file

You can load records directly from a BSON file using the `BSONLoader` class, for example:

    raw = LOAD 'file:///tmp/dump/yield_historical.in.bson' using com.mongodb.hadoop.pig.BSONLoader;

As with `MongoLoader` you can also supply an optional `idAlias` argument to map the `_id` field to a named Pig field, along with a `schema` to select fields/types to extract from the documents.

##### Datatype Mapping

In the second optional argument to the `BSONLoader` and `MongoLoader` class constructors, you can explicitly provide a datatype for each element of the schema by using the Pig schema syntax, for example `name:chararray, age:int`. If the types aren't provided, the output type will be inferred based on the values in the documents.
Data mappings used for these inferred types are as follows:

* Embedded Document/Object -> `Map`

* Array &rarr; Unnamed `Tuple`

* Date/ISODate &rarr; a 64 bit integer containing the UNIX time. This can be manipulated by Pig UDF functions to extract month, day, year, or other information - see http://aws.amazon.com/code/Elastic-MapReduce/2730 for some examples.

Note: older versions of Pig may not be able to generate mappings when tuples are unnamed, due to https://issues.apache.org/jira/browse/PIG-2509. If you get errors, try making sure that all top-level fields in the relation being stored have names assigned to them or try using a newer version of Pig.

### Writing output from Pig

#####Static BSON file output
  
To store output from Pig in a .BSON file (which can then be imported into a mongoDB instance using `mongorestore`) use the BSONStorage class. Example:

    STORE raw_out INTO 'file:///tmp/whatever.bson' USING com.mongodb.hadoop.pig.BSONStorage;

If you want to supply a custom value for '_id' in the documents written out by `BSONStorage` you can give it an optional `idAlias` field which maps a value in the Pig record to the _id field, for example:

    STORE raw_out INTO 'file:///tmp/whatever.bson' USING com.mongodb.hadoop.pig.BSONStorage('id');

The output URI for BSONStorage can be any accessible file system including `hdfs://` and `s3n://`. However, when using S3 for an output file, you will need to set `fs.s3.awsAccessKeyId` and `fs.s3.awsSecretAccessKey` for your AWS account accordingly.

#####Inserting directly into a MongoDB collection

To make each output record be used as an insert into a MongoDB collection, use the `MongoInsertStorage` class supplying the output URI. For example:

    STORE dates_averages INTO 'mongodb://localhost:27017/demo.yield_aggregated' USING com.mongodb.hadoop.pig.MongoInsertStorage('', '' );

The `MongoInsertStorage` class takes two args: an `idAlias` and a `schema` as described above.

## Usage with Hive

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

