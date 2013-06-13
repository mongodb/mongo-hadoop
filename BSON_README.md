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

* `bson.split.read_splits` - When set to `true`, will attempt to read + calculate split points for each BSON file in the input. When set to `false`, will create just *one* split for each input file, consisting of the entire length of the file. Defaults to `true`.
* `mapred.min.split.size` - Set a lower bound on acceptable size for file splits (in bytes). Defaults to 1.
* `mapred.max.split.size` - Set an upper bound on acceptable size for file splits (in bytes). Defaults to LONG_MAX_VALUE.
* `bson.split.write_splits` - Automatically save any split information calculated for input files, by writing to corresponding `.splits` files. Defaults to `true`.
* `bson.output.build_splits` - Build a `.splits` file on the fly when constructing an output .BSON file. Defaults to `false`.
* `bson.pathfilter.class` - Set the class name for a `[PathFilter](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/PathFilter.html)` to filter which files to process when scanning directories for bson input files. Defaults to `null` (no additional filtering). You can set this value to `com.mongodb.hadoop.BSONPathFilter` to restrict input to only files ending with the ".bson" extension.
