package com.mongodb.hadoop.splitter;

import com.mongodb.hadoop.input.BSONFileRecordReader;
import com.mongodb.hadoop.input.BSONFileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.io.File;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BSONFileRecordReaderTest {

    @Test
    public void testGetCurrentKey() throws Exception {
        URI path = BSONFileRecordReaderTest.class.getResource(
          "/bookstore-dump/inventory.bson").toURI();
        File file = new File(path);

        // Default case: "_id" is used as inputKey.
        BSONFileRecordReader reader = new BSONFileRecordReader();
        BSONFileSplit split = new BSONFileSplit(new Path(path), 0,
                file.length(),
                new String[0]);
        JobConf conf = new JobConf();
        TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf,
                new TaskAttemptID());
        reader.initialize(split, context);
        assertTrue(reader.nextKeyValue());
        assertEquals(reader.getCurrentKey(),
                new ObjectId("4d2a6084c6237b412fcd5597"));

        // Use a nested field as inputKey.
        reader = new BSONFileRecordReader();
        split = new BSONFileSplit(new Path(path), 0,
                file.length(),
                new String[0]);
        split.setKeyField("price.msrp");
        reader.initialize(split, context);
        assertTrue(reader.nextKeyValue());
        assertEquals(reader.getCurrentKey(), 33);

        // Use a key within an array as the inputKey.
        reader = new BSONFileRecordReader();
        split = new BSONFileSplit(new Path(path), 0,
                file.length(),
                new String[0]);
        split.setKeyField("tags.0");
        reader.initialize(split, context);
        assertTrue(reader.nextKeyValue());
        assertEquals(reader.getCurrentKey(), "html5");
    }
}
