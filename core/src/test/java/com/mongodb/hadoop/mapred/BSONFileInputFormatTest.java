package com.mongodb.hadoop.mapred;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static com.mongodb.hadoop.testutils.BaseHadoopTest.EXAMPLE_DATA_HOME;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;
import static org.junit.Assert.assertEquals;

public class BSONFileInputFormatTest {

    @Test
    public void enronEmails() throws IOException {
        BSONFileInputFormat inputFormat = new BSONFileInputFormat();
        JobConf job = new JobConf();
        job.set(INPUT_DIR, new File(EXAMPLE_DATA_HOME, "/dump/enron_mail/messages.bson").getAbsoluteFile().toURI().toString());
        FileSplit[] splits = inputFormat.getSplits(job, 5);
        int count = 0;
        BSONWritable writable = new BSONWritable();
        for (FileSplit split : splits) {
            RecordReader<NullWritable, BSONWritable> recordReader = inputFormat.getRecordReader(split, job, null);
            while (recordReader.next(null, writable)) {
                count++;
            }
        }
        assertEquals("There are 501513 messages in the enron corpus", 501513, count);
    }
}