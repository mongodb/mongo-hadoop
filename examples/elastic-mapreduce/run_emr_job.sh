
elastic-mapreduce-ruby/elastic-mapreduce --create --jobflow ENRON000 \
    --instance-type m1.xlarge \
    --bootstrap-action s3://$S3_BUCKET/emr-bootstrap-1.0.4.sh \
    --log-uri s3://$S3_BUCKET/enron_logs \
    --jar s3://$S3_BUCKET/enron-example_1.0.4-1.1.0.jar \
    --arg -D --arg mongo.job.input.format=com.mongodb.hadoop.BSONFileInputFormat \
    --arg -D --arg mapred.input.dir=s3n://$S3_BUCKET/messages.bson \
    --arg -D --arg mongo.job.mapper=com.mongodb.hadoop.examples.enron.EnronMailMapper \
    --arg -D --arg mongo.job.output.key=com.mongodb.hadoop.examples.enron.MailPair \
    --arg -D --arg mongo.job.output.value=org.apache.hadoop.io.IntWritable \
    --arg -D --arg mongo.job.partitioner= \
    --arg -D --arg mongo.job.reducer=com.mongodb.hadoop.examples.enron.EnronMailReducer \
    --arg -D --arg mongo.job.sort_comparator= \
    --arg -D --arg mongo.job.background= \
    --arg -D --arg mapred.output.file=s3n://$S3_BUCKET/enron_output.bson \
    --arg -D --arg mongo.job.output.format=com.mongodb.hadoop.BSONFileOutputFormat \
    --arg -D --arg mapred.child.java.opts=-Xmx2048m
    #--arg -D --arg mapred.task.profile=true \
