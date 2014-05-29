package com.mongodb.hadoop.mapred;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;
import static org.junit.Assert.assertEquals;

public class BSONFileInputFormatTest {

    @Test
    public void enronEmails() throws IOException {
        BSONFileInputFormat inputFormat = new BSONFileInputFormat();
        JobConf job = new JobConf();
        job.set(INPUT_DIR, new File(BaseHadoopTest.PROJECT_HOME, "examples/data/dump/enron_mail/messages.bson").getAbsolutePath());
        FileSplit[] splits = inputFormat.getSplits(job, 5);
        int count = 0;
        for (FileSplit split : splits) {
            RecordReader<NullWritable, BSONWritable> recordReader = inputFormat.getRecordReader(split, job, null);
            while (recordReader.next(null, new BSONWritable())) {
                count++;
            }
        }
        assertEquals("There are 501513 messages in the enron corpus", 501513, count);
    }
}