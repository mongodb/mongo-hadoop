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


### Updating a MongoDB collection

Just like in the MongoDB javascript shell, you can now update documents in a MongoDB collection within a Pig script via `com.mongodb.hadoop.pig.MongoUpdateStorage`. Use:
```
STORE <aliasname> INTO 'mongodb://localhost:27017/<db>.<collection>'
              	  USING com.mongodb.hadoop.pig.MongoUpdateStorage(
                        '<query>',
                        '<update>',
                        '<schema>', '<fieldtoignore>',
                        '<updateOptions>');
```
where
* `<aliasname>` is the name of the alias you want to use in updating documents in your collection
* `<db>` is the name of the database to update and `<collection>` is the name of the collection to update
* `<query>` is the (valid) JSON representing the query to use to find document(s) in the collection
* `<update>` is the (valid) JSON representing the kind of updates to perform on document(s) in the collection
* Optional: `<schema>` is the PIG schema of <aliasname>. **Strongly** advised to use this.
* Optional: `<fieldtoignore>` is the fieldname to ignore in `schema` during construction of BSON objects.
  	    Particularly useful for updating/writing an array to a document
* Optional: you can use `<updateOptions>` to provide other update options, just as in the MongoDB JS shell.
  	    For example, `{upsert : true, multi : true}`. Only upsert and multi are supported for now.

Consider the following examples:

Assume we have an alias `data` that is a bag of tuples. 
```
data =
{
 ("Bab", "Alabi", "male", 19, {("a"), ("b"), ("c")}),
 ("Dad", "Alabi", "male", 21, {("d"), ("e"))}),
 ("Tins", "Dada", "female", 50, {})
}
```
with schema `f:chararray, l:chararray, g:chararray, age:int, cars:{t:(car:chararray)}`.
**Note**: Every pig data structure in a pig schema has to be named.

To insert the gender, first and last names of each person in `data` into a `test.persons_info` collection, 
making sure that we update any existing documents with the same `first` and `last` fields, use
```
STORE data INTO 'mongodb://localhost:27017/test.persons_info'
      	   USING com.mongodb.hadoop.pig.MongoUpdateStorage(
                 '{first:"\$f", last:"\$l"}',
                 '{\$set:{gender:"\$g"}}',
                 'f:chararray, l:chararray, g:chararray, age:int, cars:{t:(car:chararray)}'
	   );
```
The resulting collection looks like this:
```
{ "_id" : ObjectId("..."), "first":"Bab", "last":"Alabi", "gender":"male"},
{ "_id" : ObjectId("..."), "first":"Dad", "last":"Alabi", "gender":"male"},
{ "_id" : ObjectId("..."), "first":"Tins", "last":"Dada", "gender":"female"}
```
Next, let's say, we want to include the `age` and `cars` for each person into the collection, use:
```
STORE data INTO 'mongodb://localhost:27017/test.persons_info'
           USING com.mongodb.hadoop.pig.MongoUpdateStorage(
                 '{first:"\$f", last:"\$l"}',
                 '{\$set:{age:"\$age"}, \$pushAll:{cars:"\$cars"}}',
                 'f:chararray, l:chararray, g:chararray, age:int, cars:{t:(car:chararray)}'
           );
```
The resulting collection looks like this:
```
{ "_id" : ObjectId("..."), "gender":"male", "age" : 19, "cars" : [{"car": "a"}, {"car":"b"}, {"car":"c"}], "first" : "Daniel", "last" : "Alabi" }
{ "_id" : ObjectId("..."), "gender":"male", "age" : 21, "cars" : [{"car":"d"}, {"car":"e"}], "first" : "Tolu", "last" : "Alabi" }
{ "_id" : ObjectId("..."), "gender":"female", "age" : 50, "cars" : [], "first" : "Tinuke", "last" : "Dada" }
```
Notice that every element in `cars` is a named map with one key `car`. In most cases, such update is unwanted/unnecessary. To instead make
`cars` an array of strings, we can use:
```
STORE data INTO 'mongodb://localhost:27017/test.persons_info'
           USING com.mongodb.hadoop.pig.MongoUpdateStorage(
                 '{first:"\$f", last:"\$l"}',
                 '{\$set:{age:"\$age"}, \$pushAll:{cars:"\$cars"}}',
                 'f:chararray, l:chararray, age:int, cars:{t:(car:chararray)}'
                 'car'
           );
```
specifying what field to ignore in the schema while inserting pig objects. The resulting collection looks like this:
```
{ "_id" : ObjectId("..."), "gender":"male", "age" : 19, "cars" : ["a", "b", "c"], "first" : "Daniel", "last" : "Alabi" }
{ "_id" : ObjectId("..."), "gender":"male", "age" : 21, "cars" : ["d", "e"], "first" : "Tolu", "last" : "Alabi" }
{ "_id" : ObjectId("..."), "gender":"female", "age" : 50, "cars" : [], "first" : "Tinuke", "last" : "Dada" }
```
More like it.



