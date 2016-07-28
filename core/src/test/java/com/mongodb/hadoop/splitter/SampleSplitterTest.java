package com.mongodb.hadoop.splitter;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoConfigUtil;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class SampleSplitterTest extends BaseHadoopTest {
    private SampleSplitter splitter = new SampleSplitter();
    private static MongoClient client = new MongoClient("localhost:27017");
    private static MongoClientURI uri =
      new MongoClientURI(
        "mongodb://localhost:27017/mongo_hadop.sample_splitter");

    @BeforeClass
    public static void setUpClass() {
        DBCollection inputCollection =
          client.getDB(uri.getDatabase())
            .getCollection(uri.getCollection());
        // Fill up with 10MB. Average object size is just over 2KB.
        StringBuilder paddingBuilder = new StringBuilder();
        for (int i = 0; i < 2048; ++i) {
            paddingBuilder.append("-");
        }
        String padding = paddingBuilder.toString();
        List<DBObject> documents = new ArrayList<DBObject>();
        for (int i = 0; i < 10 * 512; i++) {
            documents.add(
              new BasicDBObjectBuilder()
                .add("_id", i)
                .add("i", i)
                .add("padding", padding).get());
        }
        inputCollection.insert(documents);
    }

    @AfterClass
    public static void tearDownClass() {
        client.dropDatabase(uri.getDatabase());
    }

    @Test
    public void testCalculateSplits() throws SplitFailedException {
        assumeTrue(isSampleOperatorSupported(uri));
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(conf, uri.getURI());
        MongoConfigUtil.setSplitSize(conf, 1);
        splitter.setConfiguration(conf);
        List<InputSplit> splits = splitter.calculateSplits();
        assertEquals(12, splits.size());
        MongoInputSplit firstSplit = (MongoInputSplit) splits.get(0);
        assertTrue(firstSplit.getMin().toMap().isEmpty());
        MongoInputSplit lastSplit = (MongoInputSplit) splits.get(11);
        assertTrue(lastSplit.getMax().toMap().isEmpty());

        // Ranges for splits are ascending.
        int lastKey = (Integer) firstSplit.getMax().get("_id");
        for (int i = 1; i < splits.size() - 1; i++) {
            MongoInputSplit split = (MongoInputSplit) splits.get(i);
            int currentKey = (Integer) split.getMax().get("_id");
            assertTrue(currentKey > lastKey);
            lastKey = currentKey;
        }
    }

    @Test
    public void testAllOnOneSplit() throws SplitFailedException {
        assumeTrue(isSampleOperatorSupported(uri));
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(conf, uri.getURI());
        // Split size is enough to encapsulate all documents.
        MongoConfigUtil.setSplitSize(conf, 12);
        splitter.setConfiguration(conf);
        List<InputSplit> splits = splitter.calculateSplits();
        assertEquals(1, splits.size());
        MongoInputSplit firstSplit = (MongoInputSplit) splits.get(0);
        assertTrue(firstSplit.getMin().toMap().isEmpty());
        assertTrue(firstSplit.getMax().toMap().isEmpty());
    }

    @Test
    public void testAlternateSplitKey() throws SplitFailedException {
        assumeTrue(isSampleOperatorSupported(uri));
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(conf, uri.getURI());
        MongoConfigUtil.setSplitSize(conf, 1);
        MongoConfigUtil.setInputSplitKeyPattern(conf, "{\"i\": 1}");
        splitter.setConfiguration(conf);
        List<InputSplit> splits = splitter.calculateSplits();
        assertEquals(12, splits.size());
        MongoInputSplit firstSplit = (MongoInputSplit) splits.get(0);
        assertTrue(firstSplit.getMin().toMap().isEmpty());
        MongoInputSplit lastSplit = (MongoInputSplit) splits.get(11);
        assertTrue(lastSplit.getMax().toMap().isEmpty());

        // Ranges for splits are ascending.
        int lastKey = (Integer) firstSplit.getMax().get("i");
        for (int i = 1; i < splits.size() - 1; i++) {
            MongoInputSplit split = (MongoInputSplit) splits.get(i);
            int currentKey = (Integer) split.getMax().get("i");
            assertTrue(currentKey > lastKey);
            lastKey = currentKey;
        }
    }

    @Test
    public void testSampleSplitterOldMongoDB() {
        assumeFalse(isSampleOperatorSupported(uri));
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(conf, uri.getURI());
        MongoConfigUtil.setSplitSize(conf, 1);
        splitter.setConfiguration(conf);
        try {
            splitter.calculateSplits();
            Assert.fail(
              "MongoDB < 3.2 should throw SplitFailedException should fail to"
                + " use SampleSplitter.");
        } catch (SplitFailedException e) {
            // Good.
        }
    }
}
