## Configuration

#####`mongo.job.mapper`  
 sets the class to be used as the implementation for `Mapper`

#####`mongo.job.reducer` 
 sets the class to be used as the implementation for `Reducer`

#####`mongo.job.combiner`  
 sets the class to be used for a combiner. Must implement `Reducer`.

#####`mongo.job.partitioner`  
 sets the class to be used as the implementation for `Partitioner`

#####`mongo.auth.uri` 
 specify an auxiliary [mongo URI](http://docs.mongodb.org/manual/reference/connection-string/) to authenticate against when constructing splits. If you are using a sharded cluster and your `config` database requires authentication, and its username/password differs from the collection that you are running Map/Reduce on, then you may need to use this option so that Hadoop can access collection metadata in order to compute splits. An example URI: `mongodb://username:password@cyberdyne:27017/config`

#####`mongo.input.uri` 
specify an input collection (including auth and readPreference options) to use as the input data source for the Map/Reduce job. This takes the form of a 
[mongo URI](http://docs.mongodb.org/manual/reference/connection-string/), and by specifying options you can control how and where the input is read from.


**Examples**:

`mongodb://joe:12345@weyland-yutani:27017/analytics.users?readPreference=secondary` - Authenticate with "joe"/"12345" and read from only nodes in state SECONDARY from the collection "users" in database "analytics". 
`mongodb://sue:12345@weyland-yutani:27017/production.customers?readPreferenceTags=dc:tokyo,type:hadoop` - Authenticate with "joe"/"12345" and read the collection "users" in database "analytics" from only nodes tagged with "dc:tokyo" and "type:hadoop" from the collection "users" in database "analytics". 

#####`mongo.output.uri`
specify an output collection URI to write the output to. If using a sharded cluster, you can specify a space-delimited list of URIs referring to separate  mongos instances and the output records will be written to them in round-robin fashion to improve throughput and avoid overloading a single mongos.

#####`mongo.input.query` 
 filter the input collection with a query. This query must be represented in JSON format, and use the [MongoDB extended JSON format](http://docs.mongodb.org/manual/reference/mongodb-extended-json/) to represent non-native JSON data types like ObjectIds, Dates, etc.

**Example**

    'mongo.input.query={"_id":{"$gt":{"$date":1182470400000}}}'
    
    //this is equivalent to {_id : {$gt : ISODate("2007-06-21T20:00:00-0400")}} in the mongo shell


#####`mongo.input.notimeout` 
Set notimeout on the cursor when reading each split. This can be necessary if really large splits are being truncated because cursors are killed before all the data has been read.

#####`mongo.input.split.use_range_queries`

This setting causes data to be read from mongo by adding `$gte/$lt` clauses to the query for limiting the boundaries of each split, instead of the default behavior of limiting with `$min/$max`. This may be useful in cases when using `mongo.input.query` is being used to filter mapper input, but the index that would be used by `$min/$max` is less selective than the query and indexes. Setting this to `'true'` lets the MongoDB server select the best index possible and still. This defaults to '`false'` if not set.

**Limitations** 

This setting should only be used when:

* The query specified in `mongo.input.query` does not already have filters applied against the split field. Will log an error otherwise.
* The same data type is used for the split field in all documents, otherwise you may get incomplete results.
* The split field contains simple scalar values, and is not a compound key (e.g., `"foo"` is acceptable but `{"k":"foo", "v":"bar"}` is not)
  
  
#####`mongo.input.split.create_input_splits`

Defaults to `'true'`. When set, attempts to create multiple input splits from the input collection, for parallelism.
When set to `'false'` the entire collection will be treated as a **single** input split.
 
  
#####`mongo.input.split.allow_read_from_secondaries`  
**Deprecated.**

Sets `slaveOk` on all outbound connections when reading data from splits. This is deprecated - if you would like to read from secondaries, just specify the appropriate [readPreference](http://docs.mongodb.org/manual/reference/connection-string/#read-preference-options) as part of your `mongo.input.uri` setting.

#####`mongo.input.split.read_from_shards`

When reading data for splits, query the shard directly rather than via the `mongos`. This may produce inaccurate results if there are aborted/incomplete chunk migrations in process during execution of the job - to ensure correctness when using this setting, disable the balancer while running the job and also set `mongo.input.split.read_shard_chunks` to `'true'`.

#####`mongo.input.split.split_key_pattern`

If you want to customize that split point for efficiency reasons (such as different distribution) on an un-sharded collection, you may set this to any valid field name. The restriction on this key name are the *exact same rules* as when sharding an existing MongoDB Collection.  You must have an index on the field, and follow the other rules outlined in the docs.

#####` `bson.split.read_splits` - When set to `true`, will attempt to read + calculate split points for each BSON file in the input. When set to `false`, will create just *one* split for each input file, consisting of the entire length of the file. Defaults to `true`.
##### `mapred.min.split.size` - Set a lower bound on acceptable size for file splits (in bytes). Defaults to 1.

##### `mapred.max.split.size` - Set an upper bound on acceptable size for file splits (in bytes). Defaults to LONG_MAX_VALUE.

##### `bson.split.write_splits` - Automatically save any split information calculated for input files, by writing to corresponding `.splits` files. Defaults to `true`.

##### `bson.output.build_splits` - Build a `.splits` file on the fly when constructing an output .BSON file. Defaults to `false`.

##### `bson.pathfilter.class` - Set the class name for a `[PathFilter](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/PathFilter.html)` to filter which files to process when scanning directories for bson input files. Defaults to `null` (no additional filtering). You can set this value to `com.mongodb.hadoop.BSONPathFilter` to restrict input to only files ending with the ".bson" extension.

