Configuration & Behavior
========================


## Hadoop MapReduce

This package provides working *Input* and *Output* adapters for MongoDB.  You may
configure these adapters with XML or programatically. See the
WordCount examples for demonstrations of both approaches.  You can
specify a query, fields and sort specs in the XML config as JSON or
programatically as a DBObject.

### Splitting up MongoDB Source Data for the InputFormat

The MongoDB Hadoop Adapter makes it possible to create multiple
*InputSplits* on source data originating from MongoDB to
optimize/paralellize input processing for Mappers.

If `mongo.input.split.create_input_splits` is **false** (it defaults
to **true**) then MongoHadoop will use **no** splits. Hadoop will
treat the entire collection in a single, giant, *Input*.  This is
primarily intended for debugging purposes.

When true, as by default, the following possible behaviors exist:

  1. For unsharded the source collections, MongoHadoop follows the
     "unsharded split" path. (See below.)

  2. For sharded source collections:

     * If `mongo.input.split.read_shard_chunks` is **true** (defaults **true**) then we pull the chunk specs from the
       configuration server, and turn each shard chunk into an *Input Split*.  Basically, this means the mongodb sharding system does 99% of the preconfig work for us and is a good thing™

     * If `read_shard_chunks` is **false** and `mongo.input.split.read_from_shards` is **true** (it defaults to **false**) then we connect to the `mongod` or replica set for each shard individually and each shard becomes an input split. The entire content of the collection on the shard is one split. Only use this configuration in rare situations.

     * If `read_shard_chunks` is **true** and `mongo.input.split.read_from_shards` is **true** (it defaults to **false**) MongoHadoop reads the chunk boundaries from the config server but then reads data directly from the shards without using the `mongos`.  While this may seem like a good idea, it can cause erratic behavior if MongoDB balances chunks during a Hadoop job. This is not a recommended configuration for write heavy applications but may provide effective parallelism in read heavy apps.

     * If both `create_input_splits` and `read_from_shards` are **false** disabled then we pretend there is no sharding and use the "unsharded split" path. When `read_shard_chunks` is **false** MongoHadoop reads everything through mongos as a single split.

### "Unsharded Splits"

"Unsharded Splits" refers to the system that MongoHadoop uses to
calculate new splits. You may use "Unsharded splits" with sharded
MongoDB options.

This is only used:

- for unsharded collections when
  `mongo.input.split.create_input_splits` is **true**.

- for sharded collections when
  `mongo.input.split.create_input_splits` is **true** *and*
  `read_shard_chunks` is **false**.

In these cases, MongoHadoop generates multiple InputSplits. Users
have control over two factors in this system.

* `mongo.input.split_size` - Controls the maximum number of megabytes
  of each split.  The current default is 8, based on assumptions
  prior experience with Hadoop. MongoDB's default of 64 megabytes
  may be a bit too large for most deployments.

* `mongo.input.split.split_key_pattern` - Is a MongoDB key pattern
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
