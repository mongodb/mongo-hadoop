streaming
----------

Streaming support + MongoDB **requires** your Hadoop distribution include the patches for the following issues:

* [HADOOP-1722 - Make streaming to handle non-utf8 byte array](https://issues.apache.org/jira/browse/HADOOP-1722)
* [HADOOP-5450 - Add support for application-specific typecodes to typed bytes](https://issues.apache.org/jira/browse/HADOOP-5450)
* [MAPREDUCE-764 - TypedBytesInput's readRaw() does not preserve custom type codes](https://issues.apache.org/jira/browse/MAPREDUCE-764)

For the mainline Apache Hadoop distribution, these patches were merged for the 0.21.0 release.  We have verified as well that the [Cloudera](http://cloudera.com) distribution (while based on 0.20.x still) includes these patches in CDH3 Update 1; anecdotal evidence (which needs confirmation) indicates they may have been there since CDH2, and likely exist in CDH3 as well.

Building Streaming
------------------

`./sbt mongo-hadoop-streaming/assembly`

This will create a new “fat” jar in: `streaming/target/mongo-hadoop-streaming-assembly-1.1.0.jar`

This jar file is runnable with hadoop jar, and contains all of the dependencies necessary to run the job.

Each individual scripting language will have different requirements for working with MongoDB + Hadoop Streaming. Once you have the jar file built for mongo-hadoop-streaming, you will need to build and deploy the support libraries for your chosen language.

It will also be necessary to ensure these libraries are available on each Hadoop node in your cluster, along with the mongo-hadoop-core driver as outlined in the main setup instructions. However, you do not need to distribute the mongo-hadoop-streaming jar anywhere.


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
