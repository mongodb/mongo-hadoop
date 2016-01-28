package com.mongodb.hadoop.splitter;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.List;

import static com.mongodb.hadoop.splitter.MongoSplitterTestUtils.assertSplitsCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class StandaloneMongoSplitterTest {

    private static MongoClientURI uri;
    private static DBCollection collection;

    @BeforeClass
    public static void setUp() {
        MongoClient client = new MongoClient("localhost", 27017);
        uri = new MongoClientURIBuilder()
          .collection("mongo_hadoop", "splitter_test")
          .build();
        collection =
          client.getDB(uri.getDatabase()).getCollection(uri.getCollection());
        collection.drop();
        collection.createIndex("value");
        for (int i = 0; i < 40000; i++) {
            collection.insert(new BasicDBObject("_id", i).append("value", i));
        }
    }

    @Test
    public void unshardedCollection() throws UnknownHostException, SplitFailedException {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);
        MongoConfigUtil.setInputURI(config, uri);
        List<InputSplit> inputSplits = splitter.calculateSplits();
        assertFalse("Should find at least one split", inputSplits.isEmpty());

    }

    @Test
    public void unshardedCollectionMinMax()
      throws UnknownHostException, SplitFailedException {
        Configuration config = new Configuration();
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);
        MongoConfigUtil.setInputURI(config, uri);
        DBObject inputSplitKey = BasicDBObjectBuilder.start("value", 1).get();
        MongoConfigUtil.setInputSplitKey(config, inputSplitKey);
        MongoConfigUtil.setSplitSize(config, 1);
        List<InputSplit> regularSplits = splitter.calculateSplits();
        MongoConfigUtil.setMinSplitKey(config, "{value:100}");
        MongoConfigUtil.setMaxSplitKey(config, "{value:39900}");
        List<InputSplit> inputSplits = splitter.calculateSplits();
        assertTrue("should be fewer splits with min/max set",
          regularSplits.size() >= inputSplits.size());
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
    public void testFilterEmptySplitsNoQuery() throws SplitFailedException {
        Configuration config = new Configuration();
        MongoConfigUtil.setInputURI(config, uri);
        MongoConfigUtil.setEnableFilterEmptySplits(config, true);
        MongoConfigUtil.setSplitSize(config, 1);

        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);
        List<InputSplit> splits = splitter.calculateSplits();

        // No splits should be elided, because there's no query.
        for (InputSplit split : splits) {
            assertNotEquals(
              0, (((MongoInputSplit) split).getCursor().itcount()));
        }
        assertSplitsCount(collection.count(), splits);
    }

    @Test
    public void testFilterEmptySplits() throws SplitFailedException {
        Configuration config = new Configuration();
        DBObject query = new BasicDBObject(
          "$or", new BasicDBObject[]{
            new BasicDBObject("value", new BasicDBObject("$lt", 20000)),
            new BasicDBObject("value", new BasicDBObject("$gt", 35000))});
        MongoConfigUtil.setInputURI(config, uri);
        MongoConfigUtil.setEnableFilterEmptySplits(config, true);
        MongoConfigUtil.setQuery(config, query);
        // 1 MB per document results in 4 splits; the 3rd one is empty per
        // the above query.
        MongoConfigUtil.setSplitSize(config, 1);
        StandaloneMongoSplitter splitter = new StandaloneMongoSplitter(config);
        List<InputSplit> splits = splitter.calculateSplits();

        // No splits are empty.
        for (InputSplit split : splits) {
            assertNotEquals(
              0, (((MongoInputSplit) split).getCursor().itcount()));
        }
        assertSplitsCount(collection.count(query), splits);
    }
}
