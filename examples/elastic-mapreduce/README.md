##Elastic MapReduce Example

To run this example, first download the [enron data set](http://mongodb-enron-email.s3-website-us-east-1.amazonaws.com/) and put the `messages.bson` file into a bucket on S3.

Also, build the mongo-hadoop code against Hadoop 1.0.4 (see the documentation section on builds).

Export the variable S3_BUCKET to refer to the name of the s3 bucket to use as the location for code and input/output files.

Set the environment variables for `AWS_SECRET_ACCESS_KEY` and `AWS_ACCESS_KEY_ID` accordingly.


###Files

#####update_s3.sh

Run this file to place the necessary `.jar` files into your S3 bucket, and sets the permissions to be readable. 

This script requires the use of `s3cp` which you can install using `gem install s3cp` or get from [here](https://github.com/aboisvert/s3cp).


#####emr-bootstrap.sh

This is the file that will run on each node in the cluster to download dependencies, such as the Java MongoDB driver and the mongo-hadoop core code, and put them into a classpath location for Hadoop.

#####run_emr_job.sh

This script submits the job to Elastic MapReduce. Requires [elastic-mapreduce-ruby](https://github.com/tc/elastic-mapreduce-ruby).

###Source code

The Java source code for this example can be found in the directory `/examples/enron` in the source repo. 