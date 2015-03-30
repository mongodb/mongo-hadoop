package com.mongodb.hadoop.splitter;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBList;
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
import static org.junit.Assert.assertNull;

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

    @Test
    public void testCreateSplitsNoRange() throws Exception {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);

        BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject( "frame", "1500")); 
        list.add(new BasicDBObject( "frame", "1000")); 
        list.add(new BasicDBObject( "frame", "500")); 

        List<InputSplit> splits = splitter.createSplits(list, null, null);
        assertEquals(4, splits.size());

        MongoInputSplit s1 = (MongoInputSplit)splits.get(0);
        assertNull(s1.getMin().get("frame"));
        assertEquals("1500", s1.getMax().get("frame"));

        MongoInputSplit s2 = (MongoInputSplit)splits.get(1);
        assertEquals("1500", s2.getMin().get("frame"));
        assertEquals("1000", s2.getMax().get("frame"));

        MongoInputSplit s4 = (MongoInputSplit)splits.get(3);
        assertEquals("500", s4.getMin().get("frame"));
        assertNull(s4.getMax().get("frame"));
    }

    @Test
    public void testCreateSplitsRange() throws Exception {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);

        BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject( "frame", "1000")); 
        list.add(new BasicDBObject( "frame", "1500")); 
        list.add(new BasicDBObject( "frame", "2000")); 

        BasicDBObject splitMin = new BasicDBObject("frame", "500");
        BasicDBObject splitMax = new BasicDBObject("frame", "2500");
        List<InputSplit> splits = splitter.createSplits(list, splitMin, splitMax);
        assertEquals(4, splits.size());

        MongoInputSplit s1 = (MongoInputSplit)splits.get(0);
        assertEquals("500", s1.getMin().get("frame"));
        assertEquals("1000", s1.getMax().get("frame"));

        MongoInputSplit s2 = (MongoInputSplit)splits.get(1);
        assertEquals("1000", s2.getMin().get("frame"));
        assertEquals("1500", s2.getMax().get("frame"));

        MongoInputSplit s3 = (MongoInputSplit)splits.get(3);
        assertEquals("2000", s3.getMin().get("frame"));
        assertEquals("2500", s3.getMax().get("frame"));
    }

    @Test
    public void testCreateSplitsRangeDescending() throws Exception {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);

        BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject( "frame", "2000")); 
        list.add(new BasicDBObject( "frame", "1500")); 
        list.add(new BasicDBObject( "frame", "1000")); 

        BasicDBObject splitMin = new BasicDBObject("frame", "2500");
        BasicDBObject splitMax = new BasicDBObject("frame", "500");
        List<InputSplit> splits = splitter.createSplits(list, splitMin, splitMax);
        assertEquals(4, splits.size());

        MongoInputSplit s1 = (MongoInputSplit)splits.get(0);
        assertEquals("2500", s1.getMin().get("frame"));
        assertEquals("2000", s1.getMax().get("frame"));

        MongoInputSplit s2 = (MongoInputSplit)splits.get(1);
        assertEquals("2000", s2.getMin().get("frame"));
        assertEquals("1500", s2.getMax().get("frame"));

        MongoInputSplit s3 = (MongoInputSplit)splits.get(3);
        assertEquals("1000", s3.getMin().get("frame"));
        assertEquals("500", s3.getMax().get("frame"));
    }

    @Test
    public void testCreateSingleSplitAscending() throws Exception {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);

        BasicDBList list = new BasicDBList();
        BasicDBObject splitMin = new BasicDBObject("frame", "500");
        BasicDBObject splitMax = new BasicDBObject("frame", "2500");

        List<InputSplit> splits = splitter.createSplits(list, splitMin, splitMax);
        assertEquals(1, splits.size());

        MongoInputSplit s1 = (MongoInputSplit)splits.get(0);
        assertEquals("500", s1.getMin().get("frame"));
        assertEquals("2500", s1.getMax().get("frame"));
    }
}
