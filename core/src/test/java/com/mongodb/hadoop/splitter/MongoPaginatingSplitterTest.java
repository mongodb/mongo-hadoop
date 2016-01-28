package com.mongodb.hadoop.splitter;


import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.mongodb.hadoop.splitter.MongoSplitterTestUtils.assertSplitRange;
import static com.mongodb.hadoop.splitter.MongoSplitterTestUtils.assertSplitsCount;
import static org.junit.Assert.assertEquals;

public class MongoPaginatingSplitterTest {

    private static MongoCollection<Document> collection;
    private static MongoClientURI uri;

    @Before
    public void setUp() {
        uri = new MongoClientURI(
          "mongodb://localhost:27017/mongo_hadoop.pag_split_test");
        MongoClient client = new MongoClient("localhost", 27017);
        collection =
          client.getDatabase("mongo_hadoop").getCollection("pag_split_test");
        collection.drop();
        for (int i = 0; i < 40000; ++i) {
            collection.insertOne(new Document("_id", i).append("value", i));
        }
    }

    @Test
    public void testQuery() throws SplitFailedException {
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(conf, uri);
        MongoConfigUtil.setRangeQueryEnabled(conf, true);
        MongoConfigUtil.setInputSplitMinDocs(conf, 5000);
        DBObject query = new BasicDBObject(
          "$or", new BasicDBObject[]{
            new BasicDBObject("value", new BasicDBObject("$lt", 25000)),
            new BasicDBObject("value", new BasicDBObject("$gte", 31000))});
        MongoConfigUtil.setQuery(conf, query);

        MongoPaginatingSplitter splitter = new MongoPaginatingSplitter(conf);

        List<InputSplit> splits = splitter.calculateSplits();

        assertEquals(7, splits.size());
        assertSplitRange((MongoInputSplit) splits.get(0), null, 5000);
        assertSplitRange((MongoInputSplit) splits.get(1), 5000, 10000);
        assertSplitRange((MongoInputSplit) splits.get(2), 10000, 15000);
        assertSplitRange((MongoInputSplit) splits.get(3), 15000, 20000);
        assertSplitRange((MongoInputSplit) splits.get(4), 20000, 31000);
        assertSplitRange((MongoInputSplit) splits.get(5), 31000, 36000);
        assertSplitRange((MongoInputSplit) splits.get(6), 36000, null);
        // 6000 documents excluded by query.
        assertSplitsCount(collection.count() - 6000, splits);
    }

    @Test
    public void testNoQuery() throws SplitFailedException {
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(conf, uri);
        MongoConfigUtil.setRangeQueryEnabled(conf, true);
        MongoConfigUtil.setInputSplitMinDocs(conf, 5000);

        MongoPaginatingSplitter splitter = new MongoPaginatingSplitter(conf);

        List<InputSplit> splits = splitter.calculateSplits();

        assertEquals(8, splits.size());
        for (int i = 0; i < splits.size(); ++i) {
            Integer min = i == 0 ? null : i * 5000;
            Integer max = i == splits.size() - 1 ? null : (i + 1) * 5000;
            assertSplitRange((MongoInputSplit) splits.get(i), min, max);
        }
        assertSplitsCount(collection.count(), splits);
    }

}
