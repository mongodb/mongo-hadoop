package com.mongodb.hadoop.splitter;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StandaloneMongoSplitterTest {

    @Test
    public void unshardedCollection() throws UnknownHostException, SplitFailedException {
        MongoClient client = new MongoClient("localhost", 27017);
        MongoClientURI uri = new MongoClientURIBuilder()
                                 .collection("mongo_hadoop", "splitter_test")
                                 .build();
        DBCollection collection = client.getDB(uri.getDatabase()).getCollection(uri.getCollection());
        collection.drop();
        for (int i = 0; i < 10000; i++) {
            collection.insert(new BasicDBObject("_id", i)
                                  .append("value", i)
                             );
        }
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);
        MongoConfigUtil.setInputURI(config, uri);
        List<InputSplit> inputSplits = splitter.calculateSplits();
        assertFalse("Should find at least one split", inputSplits.isEmpty());

    }

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
