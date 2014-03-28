package org.mongodb.hadoop;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TestSharded extends BaseShardedTest {
    @Test
    public void testBasicInputSource() {
        assumeTrue(isSharded());
        runJob(new LinkedHashMap<String, String>(), "com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig", null, null);
        compareResults(getClient().getDB("mongo_hadoop").getCollection("yield_historical.out"), getReference());
    }

    @Test
    public void testMultiMongos() {
        assumeTrue(isSharded());
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("mongo.input.mongos_hosts", "localhost:27017 localhost:27018");
        runJob(params, "com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig", null, null);
        compareResults(getClient().getDB("mongo_hadoop").getCollection("yield_historical.out"), getReference());
    }

    @Test
    public void testMultiOutputs() {
        assumeTrue(isSharded());
        DBObject opCounterBefore1 = (DBObject) getClient().getDB("admin").command("serverStatus").get("opcounters");
        DBObject opCounterBefore2 = (DBObject) getClient2().getDB("admin").command("serverStatus").get("opcounters");
        runJob(new HashMap<String, String>(), "com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig", null,
               new String[]{"mongodb://localhost:27017/mongo_hadoop.yield_historical.out",
                            "mongodb://localhost:27018/mongo_hadoop.yield_historical.out"}
              );
        compareResults(getClient().getDB("mongo_hadoop").getCollection("yield_historical.out"), getReference());
        DBObject opCounterAfter1 = (DBObject) getClient().getDB("admin").command("serverStatus").get("opcounters");
        DBObject opCounterAfter2 = (DBObject) getClient2().getDB("admin").command("serverStatus").get("opcounters");

        compare(opCounterBefore1, opCounterAfter1);
        compare(opCounterBefore2, opCounterAfter2);
    }

    @Test
    public void testGteLt() {
        assumeTrue(isSharded());
        Map<String, String> params = new HashMap<String, String>();
        params.put("mongo.input.split.use_range_queries", "true");

        DB shard1db = getClient().getDB("mongo_hadoop");
        DB shard2db = getClient().getDB("mongo_hadoop");
        runJob(params, "com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig", null, null);
        DBCollection collection = getClient().getDB("mongo_hadoop").getCollection("yield_historical.out");
        compareResults(collection, getReference());

        collection.drop();
        params.put("mongo.input.query", "{\"_id\":{\"$gt\":{\"$date\":1182470400000}}}");
        runJob(params, "com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig", null, null);
        // Make sure that this fails when rangequery is used with a query that conflicts
        assertFalse("This collection shouldn't exist because of the failure", 
                    getClient().getDB("mongo_hadoop").getCollectionNames().contains("yield_historical.out"));
    }
    
    private void compare(final DBObject before, final DBObject after) {
        compare("update", before, after);
        compare("command", before, after);
    }

    private void compare(final String field, final DBObject before, final DBObject after) {
        Integer afterValue = (Integer) after.get(field);
        Integer beforeValue = (Integer) before.get(field);
        assertTrue(format("%s should be greater after the job runs.  before:  %d  after:  %d", field, beforeValue, afterValue),
                   afterValue > beforeValue);
    }

}
