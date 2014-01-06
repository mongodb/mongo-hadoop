package com.mongodb.hadoop.splitter;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.input.MongoInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StandaloneMongoSplitterTest {

    @Test
    public void testNullBounds() throws Exception {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);
        MongoInputSplit split = splitter.createSplitFromBounds(null, null);
        assertEquals(new BasicDBObject(), split.getMin());
        assertEquals(new BasicDBObject(), split.getMax());
    }

    @Test
    public void testNullLowerBound() throws Exception {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);
        BasicDBObject upperBound = new BasicDBObject("a", 10);
        MongoInputSplit split = splitter.createSplitFromBounds(null, upperBound);
        assertEquals(new BasicDBObject(), split.getMin());
        assertEquals(10, split.getMax().get("a"));
    }

    @Test
    public void testNullUpperBound() throws Exception {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);
        BasicDBObject lowerBound = new BasicDBObject("a", 10);
        MongoInputSplit split = splitter.createSplitFromBounds(lowerBound, null);
        assertEquals(10, split.getMin().get("a"));
        assertEquals(new BasicDBObject(), split.getMax());
    }

    @Test
    public void testLowerUpperBounds() throws Exception {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);
        BasicDBObject lowerBound = new BasicDBObject("a", 0);
        BasicDBObject upperBound = new BasicDBObject("a", 10);
        MongoInputSplit split = splitter.createSplitFromBounds(lowerBound, upperBound);
        assertEquals(0, split.getMin().get("a"));
        assertEquals(10, split.getMax().get("a"));
    }
}
