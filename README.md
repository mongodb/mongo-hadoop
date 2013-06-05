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

######`mongo.output.uri`
specify an output collection URI to write the output to. If using a sharded cluster, you can specify a space-delimited list of URIs referring to separate  mongos instances and the output records will be written to them in round-robin fashion to improve throughput and avoid overloading a single mongos.

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

If you want to customize that split point for efficiency reasons (such as different distribution) on an un-sharded collection, you may set this to any valid field name. The restriction on this key name are the *exact same rules* as when sharding an existing MongoDB Collection.  You must have an index on the field, and follow the other rules outlined in the docs.

 
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
        	yield {'_id': doc['user_id'], 'city': doc['location']['city']}

    BSONMapper(mapper)

#####Reducer
To implement a reducer, write a function which accepts two arguments: a key and an iterable sequence of documents matching that key. Compute your reduce output and pass it back to Hadoop with `return`. Then call `BSONReducer()` against this function. For example,

    from pymongo_hadoop import BSONReducer

    def reducer(key, values):
        _count = _sum = 0
        for v in values:8
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

## Example 1 - Treasury Yield Calculation

###Setup

Source code is in `examples/treasury_yield`. To prepare the sample data, first run `mongoimport` with the file in `examples/treasury_yield/src/main/resources/yield_historical_in.json` to load it into a local instance of MongoDB.

We end up with a test collection containing documents that look like this:

    { 
      "_id": ISODate("1990-01-25T19:00:00-0500"), 
      "dayOfWeek": "FRIDAY", "bc3Year": 8.38,
      "bc10Year": 8.49,
      …
    }

###Map/Reduce with Java

The goal is to find the average of the bc10Year field, across each year that exists in the dataset. First we define a mapper, which is executed against each document in the collection. We extract the year from the `_id` field and use it as the output key, along with the value we want to use for averaging, `bc10Year`.

	public class TreasuryYieldMapper 
	    extends Mapper<Object, BSONObject, IntWritable, DoubleWritable> {
	
	    @Override
	    public void map( final Object pKey,
	                     final BSONObject pValue,
	                     final Context pContext )
	            throws IOException, InterruptedException{
	        final int year = ((Date)pValue.get("_id")).getYear() + 1900;
	        double bid10Year = ( (Number) pValue.get( "bc10Year" ) ).doubleValue();
	        pContext.write( new IntWritable( year ), new DoubleWritable( bid10Year ) );
	    }
	}

Then we write a reducer, a function which takes the values collected for each key (the year)  and performs some aggregate computation of them to get a result.

	public class TreasuryYieldReducer
	        extends Reducer<IntWritable, DoubleWritable, IntWritable, BSONWritable> {
	    @Overrideyouyour
	    public void reduce( final IntWritable pKey,
	                        final Iterable<DoubleWritable> pValues,
	                        final Context pContext )
	            throws IOException, InterruptedException{
	        int count = 0;
	        double sum = 0;
	        for ( final DoubleWritable value : pValues ){
	            sum += value.get();
	            count++;
	        }
	
	        final double avg = sum / count;
		
	        BasicBSONObject output = new BasicBSONObject();
	        output.put("avg", avg);
	        pContext.write( pKey, new BSONWritable( output ) );
	    }	
	}
	
###Pig

We can also easily accomplish the same task with just a few lines of Pig script. We also use some external UDFs provided by the Amazon Piggybank jar: http://aws.amazon.com/code/Elastic-MapReduce/2730

    -- UDFs used for date parsing
	REGISTER /tmp/piggybank-0.3-amzn.jar
	-- MongoDB Java driver
	REGISTER  /tmp/mongo-2.10.1.jar;
	-- Core Mongo-Hadoop Library
	REGISTER ../core/target/mongo-hadoop-core_1.0.3-1.1.0-SNAPSHOT.jar
	-- mongo-hadoop pig support
	REGISTER ../pig/target/mongo-hadoop-pig_1.0.3-1.1.0-SNAPSHOT.jar
	
	raw = LOAD 'mongodb://localhost:27017/demo.yield_historical.in' using com.mongodb.hadoop.pig.MongoLoader; 
	DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
	DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();
	
	date_tenyear = foreach raw generate UnixToISO($0#'_id'), $0#'bc10Year';
	parsed_year = foreach date_tenyear generate 
	    FLATTEN(EXTRACT($0, '(\\d{4})')) AS year, (double)$1 as bc;
	
	by_year = GROUP parsed_year BY (chararray)year;
	year_10yearavg = FOREACH by_year GENERATE group, AVG(parsed_year.bc) as tenyear_avg;
	
	-- Args to MongoInsertStorage are: schema for output doc, field to use as '_id'.
	STORE year_10yearavg 
	 INTO 'mongodb://localhost:27017/demo.asfkjabfa' 
	 USING		
	 com.mongodb.hadoop.pig.MongoInsertStorage('group:chararray,tenyear_avg:float', 'group');



## Example 2 - Enron E-mails

###Setup

Download a copy of the data set [here](http://mongodb-enron-email.s3-website-us-east-1.amazonaws.com/). Each document in the data set contains a single e-mail, including headers containing sender and recipient information. In this example we will build a list of the unique sender/recipient pairs, counting how many times each pair occurs.

Abbreviated code snippets shown below - to see the full source for this example, please see [here](http://github.com/mongodb/mongo-hadoop/examples/enron/src/) 

####Map/Reduce with Java

The mapper class will get the `headers` field from each document, parse out the sender from the `From` field and the recipients from the `To` field, and construct a `MailPair` object containing each pair which will act as the key. Then we emit the value `1` for each key. `MailPair` is just a simple "POJO" that contains Strings for the `from` and `to` values, and implements `WritableComparable` so that it can be serialized across Hadoop nodes and sorted. 	
	
	@Override
	public void map(NullWritable key, BSONObject val, final Context context)
        throws IOException, InterruptedException{
		if(val.containsKey("headers")){
			BSONObject headers = (BSONObject)val.get("headers");
			if(headers.containsKey("From") && headers.containsKey("To")){
				String from = (String)headers.get("From");
				String to = (String)headers.get("To");
                String[] recips = to.split(",");
                for(int i=0;i<recips.length;i++){
                    String recip = recips[i].trim();
                    if(recip.length() > 0){
                        context.write(new MailPair(from, recip), new IntWritable(1));
                    }
                }
			}
		}
	}


The reduce class will take the collected values for each key, sum them together, and record the output.

    @Override
    public void reduce( final MailPair pKey,
                        final Iterable<IntWritable> pValues,
                        final Context pContext )
            throws IOException, InterruptedException{
        int sum = 0;
        for ( final IntWritable value : pValues ){
            sum += value.get();
        }
        BSONObject outDoc = new BasicDBObjectBuilder().start().add( "f" , pKey.from).add( "t" , pKey.to ).get();
        BSONWritable pkeyOut = new BSONWritable(outDoc);
        pContext.write( pkeyOut, new IntWritable(sum) );
    }

####Pig

To accomplish the same with pig, but with much less work:

	REGISTER ../mongo-2.10.1.jar;
	REGISTER ../core/target/mongo-hadoop-core_cdh4.3.0-1.1.0.jar
	REGISTER ../pig/target/mongo-hadoop-pig_cdh4.3.0-1.1.0.jar
	
	raw = LOAD 'file:///Users/mike/dump/enron_mail/messages.bson' using com.mongodb.hadoop.pig.BSONLoader('','headers:[]') ; 
	send_recip = FOREACH raw GENERATE $0#'From' as from, $0#'To' as to;
	send_recip_filtered = FILTER send_recip BY to IS NOT NULL;
	send_recip_split = FOREACH send_recip_filtered GENERATE from as from, FLATTEN(TOKENIZE(to)) as to;
	send_recip_split_trimmed = FOREACH send_recip_split GENERATE from as from, TRIM(to) as to;
	send_recip_grouped = GROUP send_recip_split_trimmed BY (from, to);
	send_recip_counted = FOREACH send_recip_grouped GENERATE group, COUNT($1) as count;
	STORE send_recip_counted INTO 'file:///tmp/enron_emailcounts.bson' using com.mongodb.hadoop.pig.BSONStorage;


## Usage with Amazon Elastic MapReduce

Amazon Elastic MapReduce is a managed Hadoop framework that allows you to submit jobs to a cluster of customizable size and configuration, without needing to deal with provisioning nodes and installing software.

Using EMR with mongo-hadoop allows you to run MapReduce jobs against mongo backup files stored in S3.

Submitting jobs using mongo-hadoop to EMR simply requires that the bootstrap actions fetch the dependencies (mongoDB java driver, mongo-hadoop-core libs, etc.) and place them into the hadoop distributions `lib` folders.

For a full example (running the enron example on Elastic MapReduce) please see [here](examples/elastic-mapreduce/README.md).

## Usage with Pig

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

**Note**: Pig 0.9 and earlier have issues with non-named tuples. You may need to unpack and name the tuples explicitly, for example: 
The tuple `(1,2,3)` can not be transformed into a MongoDB document. But,
`FLATTEN((1,2,3)) as v1, v2, v3` can successfully be stored as `{'v1': 1, 'v2': 2, 'v3': 3}`
Pig 0.10 and later handles both cases correctly, so avoiding Pig 0.9 or earlier is recommended.



##### From a .BSON file

You can load records directly into a Pig relation from a BSON file using the `BSONLoader` class, for example:

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

If writing to a MongoDB instance, it's recommended to set `mapred.map.tasks.speculative.execution=false` and `mapred.reduce.tasks.speculative.execution=false` to prevent the possibility of duplicate records being written. You can do this on the command line with `-D` switches or directly in the Pig script using the `SET` command.

#####Static BSON file output
  
To store output from Pig in a .BSON file (which can then be imported into a mongoDB instance using `mongorestore`) use the BSONStorage class. Example:

    STORE raw_out INTO 'file:///tmp/whatever.bson' USING com.mongodb.hadoop.pig.BSONStorage;

If you want to supply a custom value for the `'_id'` field in the documents written out by `BSONStorage` you can give it an optional `idAlias` field which maps a value in the Pig record to the `'_id'` field in the output document, for example:

    STORE raw_out INTO 'file:///tmp/whatever.bson' USING com.mongodb.hadoop.pig.BSONStorage('id');

The output URI for BSONStorage can be any accessible file system including `hdfs://` and `s3n://`. However, when using S3 for an output file, you will also need to set `fs.s3.awsAccessKeyId` and `fs.s3.awsSecretAccessKey` for your AWS account accordingly.

#####Inserting directly into a MongoDB collection

To make each output record be used as an insert into a MongoDB collection, use the `MongoInsertStorage` class supplying the output URI. For example:

    STORE dates_averages INTO 'mongodb://localhost:27017/demo.yield_aggregated' USING com.mongodb.hadoop.pig.MongoInsertStorage('', '' );

The `MongoInsertStorage` class also takes two args: an `idAlias` and a `schema` as described above. If `schema` is left blank, it will attempt to infer the output schema from the data using the strategy described above. If `idAlias` is left blank, an `ObjectId` will be generated for the value of the `_id` field in each output document.

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

