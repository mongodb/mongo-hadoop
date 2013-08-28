Hive Support
============
By default, any table created in Hive is HDFS-based; that is, the metadata and underlying rows of data associated with the table is stored in HDFS. Mongo-Hadoop now supports the creation of MongoDB-based Hive tables and BSON-based Hive tables. Both MongoDB-based Hive tables and BSON-based Hive tables can be:

* Queried just like HDFS-based Hive tables. 
* Combined with HDFS-based Hive tables in joins and sub-queries




## Requirements

#### Supported Hadoop and Hive versions

As of August 2013, only Hive versions <= 0.10 are stable. Mongo-Hadoop currently supports Hive versions >= 0.9. Some classes and functions are deprecated in Hive 0.11, but they're still functional.

Hadoop versions greater than 0.20.x are supported. CDH4 is supported, but CDH3 with its native Hive 0.7 is not. However, CDH3 is compatible with newer versions of Hive. Installing a non-native version with CDH3 can be used with Mongo-Hadoop.

#### Building Packages

The command `sbt package` in the mongo-hadoop base directory will build all the mongo-hadoop packages for its core, pig, and hive support. You can also use
`sbt mongo-hadoop-hive/package` to build only the Hive support package.




## Quickstart Example

```
CREATE TABLE individuals
( 
  id INT,
  name STRING,
  age INT,
  work STRUCT<title:STRING, hours:INT>
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES('mongo.columns.mapping'='{"id":"_id","work.title":"job.position"}')
TBLPROPERTIES('mongo.uri'='mongodb://localhost:27017/test.persons');
```
The new table **individual** is a MongoDB-based Hive table. The table can now be queried within Hive just like a HDFS-based Hive table:

```
SELECT name, age
FROM individuals
WHERE id > 100;
```




## Connecting to MongoDB - MongoStorageHandler

The creation of MongoDB-based Hive tables is handled by **MongoStorageHandler**. It also handles retrieving data from such tables (using `select`) and storing data into such tables (using `insert`). 

```
CREATE [EXTERNAL] TABLE <tablename>
(<schema>)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
[WITH SERDEPROPERTIES('mongo.columns.mapping'='<JSON mapping>')]
TBLPROPERTIES('mongo.uri'='<MongoURI>');
```

You must specify what collection in MongoDB should correspond to the MongoDB-based Hive table, using the `mongo.uri` parameter in `TBLPROPERTIES`. `<MongoURI>` has to be a valid MongoDB connection URI string. See [this](http://docs.mongodb.org/manual/reference/connection-string/) for more details. The referenced collection doesn't have to empty when creating a corresponding Hive table. 

Optionally, you can specify a correspondence to map certain Hive columns and Hive struct fields to specific MongoDB fields in the underlying collection, using the `mongo.columns.mapping` option in SERDEPROPERTIES. This option is also available upon creation of a BSON-based Hive table (see below).

If the table created is `EXTERNAL`, when the table is dropped only its metadata is deleted; the underlying MongoDB collection remains intact. On the other hand, if the table is not `EXTERNAL`, dropping the table deletes both the metadata associated with the table and the underlying MongoDB collection.

#### Limitations of MongoStorageHandler

* **INSERT INTO vs. INSERT OVERWRITE**: As of now, there's no way for a table created with any custom StorageHandler to distinguish between the INSERT INTO TABLE and INSERT OVERWRITE commands. So both commands do the same thing: insert certain rows of data into a MongoDB-based Hive table. So to INSERT OVERWRITE, you'd have to first drop the table and then insert into the table.



## Using BSON files - STORED AS (Specified SerDe, INPUT and OUTPUT)

```
CREATE [EXTERNAL] TABLE <tablename>
(<schema>)
ROW FORMAT SERDE 'com.mongodb.hadoop.hive.BSONSerDe'
[WITH SERDEPROPERTIES('mongo.columns.mapping'='<JSON mapping>')]
STORED AS INPUTFORMAT 'com.mongodb.hadoop.mapred.BSONFileInputFormat'
OUTPUTFORMAT 'com.mongodb.hadoop.hive.output.HiveBSONFileOutputFormat'
[LOCATION '<path to existing directory>'];
```

Instead of using a StorageHandler to read, serialize, deserialize, and output the data from Hive objects to BSON objects, the individual components are listed individually. This is because using a StorageHandler has too many negative effects when dealing with the native HDFS filesystem (see below). 

The optional LOCATION keyword can be used to specify a directory in HDFS where the data for the table should be stored. Without it, the table is automatically created at `/user/hive/warehouse` under the table name. The LOCATION at the creation of the table can either be empty or hold existing BSON. However, it must only have BSON, else during deserialization the splits will be calculated incorrectly.

Note that for Elastic MapReduce, the S3 filesystem is coupled with HDFS so that Hive in Elastic MapReduce will compute over the S3 filesystem the same as it does over HDFS. All LOCATIONs specified during table creation should point to the bucket where the table data resides and where it should be outputted. Elastic MapReduce automatically runs with the latest versions of Hive and Hadoop which is compatible with Mongo-Hadoop.

#### Reasons for not using a StorageHandler for BSON 

1. Using a StorageHandler will mark the table as non-native. Data cannot be loaded into the table directory from a "LOAD DATA" command. Instead, the data can only come from the result of a query.
2. Files are only stored as a single output file, not one per reduce call. This will cause much lock leasing problems.
3. INSERT INTO a StorageHandler table behaves as INSERT OVERWRITE.





## Serialization and Deserialization

The data stored in MongoDB-based and BSON-based tables must be deserialized into Hive objects when reading and vice versa when writing. This is handled by the BSONSerDe (which stands for BSONSerializerDeserializer).

If the type of any Hive field does not correpond to the data type of the underlying collection, then `null` will appear as the data for that field.

The primitive type conversions are quite straightforward, but there are a couple complex object type conversions to keep in mind:

| Hive object | MongoDB object |
| :---------: | :------------: |
| STRUCT | embedded document |
| MAP | embedded document |
| ARRAY | array |
| STRUCT<oid:STRING, bsontype:INT> | ObjectId |

##### STRUCT 

The Hive STRUCT is the equivalent of the embedded document in MongoDB. A STRUCT will declare inner fields and their types, both of which should correspond to the embedded document.

##### MAP 

A MAP is similar to a STRUCT except the values are all of the same type. This is not a restriction placed on any MongoDB embedded document, but MAP is a valid type to serialize to and from if the MongoDB embedded document values are all of the same type.

##### Array

A Hive ARRAY demands the declaration of its data types, and all its values must have the same data type. The Hive ARRAY corresponds to the MongoDB array, except the MongoDB array elements must of the same type.

##### ObjectId 

Having an ObjectId correspond to a special instance of a STRUCT was a design decision focused on differentiating an ObjectId from any other value types. A Hive field corresponding to an ObjectId must be a STRUCT with the fields `oid`, a STRING, and `bsontype`, an INT, and nothing else. The `oid` is the string of the ObjectId while the `bsontype` should always be [8](http://docs.mongodb.org/manual/faq/developers/#what-is-the-compare-order-for-bson-types). 

The BSONSerDe will automatically cast a MongoDB ObjectId into this type of a STRUCT during deserialization, and this type of a STRUCT will automatically be returned as an ObjectId during serialization.





## BSONSerDe Mappings

Specifying SerDe mappings is optional. Without it, all the field names in the Hive table are expected to be the same as in the underlying MongoDB collection. If the exact field names in Hive cannot be found in a MongoDB document, their values in the Hive table will be `null`.

The `mongo.columns.mapping` parameter should be set to a JSON string whose keys correspond to certain Hive columns or Hive table struct field names. The values should correspond to certain field names in MongoDB. This functionality is better illustrated with an example. 

Suppose we have a MongoDB collection `persons` in the database `test` that has the following entries:

```
{
    _id : 1,
    _Personal : {
        _Name : {
            _F : "Sweet", 
            l : "Song"
        },
        _Class : {
            year : 2013
        }
    }, 
    _Relatives : [
        {
            _F : "Mike",
            l : "O'Brien"
        }
    ], 
    age : 22
}
{
    _id : 2,
    _Personal : {
        _Name : {
            _F : "Daniel", 
            l : "Alabi"
        },
        _Class : {
            year : 2014
        }
    }, 
    _Relatives : [
        {
            _F : "Sweet",
            l : "Song"
        }
    ], 
    age : 19
}
```

We would like to create a Hive table that corresponds to the `persons` collection. But Hive doesn't allow column and struct field names to start with `_`. No qualms, we can do this:

```
CREATE TABLE persons
(
id INT,
personal STRUCT<
      name:STRUCT<f:STING, l:STRING>>,
      class:STUCT<year:INT>
      >,
relatives ARRAY<STRUCT<f:STRING, l:STRING>>,
age INT           
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES('mongo.columns.mapping'='{"id":"_id", "relatives.f":"_Relatives._F", "personal.name.f":"_Personal._Name._F", "personal.class":"_Personal._Class"}')
TBLPROPERTIES('mongo.uri'='mongodb://localhost:27017/test.persons')
```

At this point, the `persons` collection and the `persons` Hive table both mirror exactly the same data, even though not all the mappings were specified. This is because some upper level mappings like `relatives:_Relatives, personal:_Personal, personal.name:_Personal._Name` were inferred based on the available mappings. In addition, the syntax for defining mappings for STRUCT fields in Hive arrays is exactly the same as for plain STRUCT fields because ARRAYs in Hive can store only one type of object, conveniently.


If a mapping isn't specified for a particular field name in Hive, the BSONSerDe assumes that the corresponding field name in the underlying MongoDB collection specified is the same. That's why we didn't have to specify a mapping for `age`, `relatives.l`, `personal.name.l` and `personal.class.year`.


Because the BSONSerDe mapper tries to infer upper level mappings from any multi-level mapping, the Hive struct field or column name has to be of the same depth as the MongoDB field being mapped to. So, we can't have a mapping like `a.b.c : d.e.f.g` because the upper level mappings `a:d, a.b:d.e, a.b.c:d.e.f` are created, but it's unclear what `d.e.f.g` should be mapped from. In the same vain, we can't have a mapping like `a.b.c.d : e.f.g` because the upper level mappings `a:e, a.b:e.f, a.b.c:e.f.g` are created, but it's unclear what `a.b.c.d` should be mapped to.


## Key Uniqueness
Hive tables, natively, don't have key uniqueness and normalization features; that is, there's no concept of primary keys in Hive. So there might be several rows with the same data. For so many cases, this is not ideal. Uniqueness of rows can be ensured in MongoDB-based and BSON-based Hive tables by mapping a Hive column to the MongoDB `_id` field, using the `mongo.columns.mapping` SerDe parameter.

## A More Detailed Example
We also provide a more detailed example use case of the Hive support in Mongo-Hadoop: `examples/enron/hive`.

## ROADMAP
* Indexing: Indexing when connecting to a MongoDB instance, must be managed in the MongoDB client, not in Hive.
* Partitioning: It'd be nice to able to partition MongoDB-based Hive tables just as HDFS-based Hive tables.
