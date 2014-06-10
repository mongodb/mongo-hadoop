package com.mongodb.hadoop;

import com.mongodb.DBCollection;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class TestStreaming extends TreasuryTest {
    @Before
    public void hadoopVersionCheck() {
        assumeFalse(HADOOP_VERSION.startsWith("1.0"));
        assumeFalse(isSharded(getInputUri()));
    }

    @Test
    public void testBasicStreamingJob() {
        Map<String, String> params = new TreeMap<String, String>();
        params.put(MongoConfigUtil.INPUT_QUERY, "{_id:{$gt:{$date:883440000000}}}");
        new StreamingJob()
            .params(params)
            .inputUris(getInputUri())
            .outputUris(getOutputUri())
            .execute();
        
        DBCollection collection = getClient(getInputUri()).getDB("mongo_hadoop").getCollection("yield_historical.out");
        assertEquals(14, collection.count());
    }
}
