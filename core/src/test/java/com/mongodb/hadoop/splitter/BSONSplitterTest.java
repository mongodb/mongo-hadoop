package com.mongodb.hadoop.splitter;

import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BSONSplitterTest {

    @Test
    public void testGetSplitsFilePath() {
        Configuration conf = new Configuration();
        Path bsonFilePath = new Path("data.bson");

        // No explicit configuration.
        assertEquals(
          new Path(".data.bson.splits"),
          BSONSplitter.getSplitsFilePath(bsonFilePath, conf)
        );

        // Explicit configuration.
        MongoConfigUtil.setBSONSplitsPath(conf, "/foo/bar");
        assertEquals(
          new Path("/foo/bar/.data.bson.splits"),
          BSONSplitter.getSplitsFilePath(bsonFilePath, conf)
        );
    }
}
