package com.mongodb.hadoop.splitter;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ShardChunkMongoSplitterTest extends BaseHadoopTest {

    private ShardChunkMongoSplitter splitter = new ShardChunkMongoSplitter();

    private DBObject createChunk(final String key, final Object min, final Object max, final String shardName) {
        return new BasicDBObjectBuilder()
          .push("min").add(key, min).pop()
          .push("max").add(key, max).pop()
          .append("shard", shardName).get();
    }

    @Test
    public void testSplitPreferredLocations()
      throws SplitFailedException, IOException, InterruptedException {
        // Create list of chunks.
        List<DBObject> chunksList = new ArrayList<DBObject>(){{
            add(createChunk("i", new MinKey(), 500, "sh01"));
            add(createChunk("i", 500, new MaxKey(), "sh02"));
        }};
        // Create shards map.
        Map<String, List<String>> shardsMap = new HashMap<String, List<String>>() {{
            put("sh01", Arrays.asList("mongo.sh01.dc1:27017", "mongo.sh01.dc2:27017"));
            put("sh02", Arrays.asList("mongo.sh02.dc1:27027", "mongo.sh02.dc2:27027"));
        }};

        Configuration conf = new Configuration();
        MongoConfigUtil.setInputMongosHosts(
          conf, Arrays.asList("mongo.sh01.dc1:27018", "mongo.sh02.dc2:27018"));
        MongoConfigUtil.setInputURI(
          conf, new MongoClientURI("mongodb://mongo.dc1:27018,mongo.dc2:27018/hadoop.test"));
        splitter.setConfiguration(conf);

        List<InputSplit> splits = splitter.calculateSplitsFromChunks(chunksList, shardsMap);
        assertEquals("mongo.sh01.dc1:27018", splits.get(0).getLocations()[0]);
        assertEquals("mongo.sh02.dc2:27018", splits.get(1).getLocations()[0]);
    }
}
