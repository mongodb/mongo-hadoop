package com.mongodb.hadoop.splitter;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MongoSplitterFactoryTest extends BaseHadoopTest {
    private static MongoClientURI uri =
      new MongoClientURI(
        "mongodb://localhost:27017/mongo_hadoop.splitter_factory_test");
    private static MongoClient client;

    @BeforeClass
    public static void setUpClass() {
        client = new MongoClient(uri);
        // Create collection so that we can call "collstats" on it.
        client.getDatabase(uri.getDatabase())
          .getCollection(uri.getCollection()).insertOne(
          new Document("this collection exists now", true));
    }

    @AfterClass
    public static void tearDownClass() {
        client.dropDatabase(uri.getDatabase());
    }

    @Test
    public void testDefaultSplitter() {
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(conf, uri);
        MongoSplitter defaultSplitter = MongoSplitterFactory.getSplitter(conf);
        if (isSharded(uri)) {
            assertTrue(defaultSplitter instanceof ShardChunkMongoSplitter);

            MongoConfigUtil.setReadSplitsFromShards(conf, true);
            assertTrue(
              MongoSplitterFactory.getSplitter(conf)
                instanceof ShardMongoSplitter);
        } else {
            if (isSampleOperatorSupported(uri)) {
                assertTrue(defaultSplitter instanceof SampleSplitter);
            } else {
                assertTrue(defaultSplitter instanceof StandaloneMongoSplitter);
            }
        }
    }
}
