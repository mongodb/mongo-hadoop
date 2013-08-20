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

## STORED AS -- Specified SerDe, INPUT and OUTPUT

## ROADMAP
* Indexing and Partitioning
