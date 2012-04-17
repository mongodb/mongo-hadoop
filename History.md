
1.0.0 / 2012-04-09 
==================

  * Fixed file distribution for streaming addon files
  * Fixed Thrift dep for cdh3.
  * Add treasury yield example build support.
  * Added a Streaming Example M/R job with enron email corpus
  * HADOOP-29 - removes excessive logging for each tuple stored in MongoDB (RJurney)
  * Streaming: Add support for python generators in reduce functions (MLew)
  * Pig: Fix for exporting tuples to mongodb as map
  * Fixed CDH4 build flags to correct compilation step.
  * Fixed Hadoop build for dependencies across versions.
  * Added a "load-sample-data" task to use for loading samples into mongo for testing/demos
  * Hadoop 0.22.x support now works for those who need it (although I believe it's a deprecated branch)
  * Stock Apache 0.23.x now builds, using the actual 0.23.1 release...  insanity around the MapReduce dep
  * added twitter hashtag examples
  * Relocate Pymongo_Hadoop module to a new "language_Support" subdirectory. Created a setup.py file to build an egg / package. Available on PyPi as 'pymongo_hadoop'.
  * Fixed pymongo_hadoop output to use BSON.encode
  * Added support to streaming for the -file flag to distribute files out to the cluster if they don't exist.
  * Make InputFormat and OutputFormat implied on Streaming jobs, defaulting to the Mongo ones.
  * Streaming now builds as a fat assembly jar and works.
  * Added an 0.23 / cdh4 build.  No longer allow raw "cdh" or "cloudera" build artifacts to avoid confusion as to 'which cloudera?'
  * Added a .23 build, based on Cloudera's current distro (should be binary compatible with stock)
  * If combiner is not specified, do not pass it to Hadoop.  While the combiner should be optional, giving Hadoop a null combiner will result in a NullPointerException.

1.0.0-rc0 / 2012-02-12 
==================

  * Initial Release, Release Candidate
