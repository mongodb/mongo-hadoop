Usage with Hive
===============

## Quickstart Example

```
CREATE TABLE individual
( 
  id STRUCT<oid:STRING, bsonType:INT>,
  name STRING,
  age INT,
  work STRUCT<title:STRING, salary:INT>
)
STORED BY "com.mongodb.hadoop.hive.MongoStorageHandler"
WITH SERDEPROPERTIES("mongo.columns.mapping"="{'id':'_id','work.title':'job.position'}")
TBLPROPERTIES("mongo.uri"="mongodb://localhost:27017/test.person");
```

## Mapping

## STORED BY 

### HiveMongoStorageHandler

#### `mongo.uri`

### HiveBSONStorageHandler

StorageHandler is a convenient way to manage tables in hdfs. The syntax is very similar to the MongoStorageHandler, except there's no need for TBLPROPERTIES (`mongo.uri`) and there's the option to specify a LOCATION for the input and output files to be stored in hdfs aside from the default Hive directories. Notice that the `mongo.columns.mapping` is also supported in BSONStorageHandler.

```
CREATE TABLE individual
( 
  id STRUCT<oid:STRING, bsonType:INT>,
  name STRING,
  age INT,
  work STRUCT<title:STRING, salary:INT>
)
STORED BY "com.mongodb.hadoop.hive.BSONStorageHandler"
WITH SERDEPROPERTIES("mongo.columns.mapping"="{'id':'_id','work.title':'job.position'}")
LOCATION "/temp/individual";

```

Using a StorageHandler will mark the table as non-native. There are two major defects with this.
1. Data cannot be load into the table directory from a "LOCAL DATA" command. Instead, the data can only come from the result of a query.
2. Files are only stored as a single output file, not one per reduce call. This will cause much lock leasing problems.

The alternative is to use STORED AS (individual components) instead of STORED BY (a StorageHandler).

## STORED AS -- Specified SerDe, INPUT and OUTPUT

A table created with SerDe, INPUTFORMAT, OUTPUTFORMAT specified in STORED AS will be treated as a native table. The files are for the most part the same between STORED BY and STORED AS, but they must be specified individually during table creation. 

Note that Elastic MapReduce will only output to tables created with STORED AS. S3 is coupled with hdfs so that the S3 filesystem replaces the hdfs filesystem. All LOCATIONS specified in table creation should point to the bucket where the table resides and where it should be outputted.

```
CREATE TABLE individual
( 
  id STRUCT<oid:STRING, bsonType:INT>,
  name STRING,
  age INT,
  work STRUCT<title:STRING, salary:INT>
)
ROW FORMAT SERDE "com.mongodb.hadoop.hive.BSONSerDe"
WITH SERDEPROPERTIES("mongo.columns.mapping"="{'id':'_id','work.title':'job.position'}")
STORED AS INPUTFORMAT "com.mongodb.hadoop.mapred.BSONFileInputFormat"
OUTPUTFORMAT "com.mongodb.hadoop.hive.output.HiveBSONFileOutputFormat"
LOCATION "/temp/individual";
```

## ROADMAP
* Indexing and Partitioning - to be developed. Indexing when connecting to a MongoDB instance, must be managed in the MongoDB client, not in Hive.
