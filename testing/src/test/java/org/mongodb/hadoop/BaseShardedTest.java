package org.mongodb.hadoop;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

public class BaseShardedTest extends BaseHadoopTest {
    private static final Log LOG = LogFactory.getLog(BaseShardedTest.class);
    private MongoClient client2;

    @Before
    public void shuffleChunks() throws IOException, InterruptedException, TimeoutException {
        LOG.info("shuffling chunks across shards");
        if(!isSharded()) {
            return;
        }
        
        DB adminDB = getClient().getDB("admin");
        adminDB.command(new BasicDBObject("enablesharding", "mongo_hadoop"));

        getClient().getDB("config").getCollection("settings").update(new BasicDBObject("_id", "balancer"),
                                                                new BasicDBObject("$set", new BasicDBObject("stopped", true)), false, true);
        adminDB.command(new BasicDBObject("shardCollection", "mongo_hadoop.yield_historical.in")
                            .append("key", new BasicDBObject("_id", 1)));

        DBCollection historicalIn = getClient().getDB("mongo_hadoop").getCollection("yield_historical.in");

        for (int chunkpos : new int[]{2000, 3000, 1000, 500, 4000, 750, 250, 100, 3500, 2500, 2250, 1750}) {
            Object middle = historicalIn.find().sort(new BasicDBObject("_id", 1)).skip(chunkpos).iterator().next().get("_id");
            adminDB.command(new BasicDBObject("split", "mongo_hadoop.yield_historical.in")
                                .append("middle", new BasicDBObject("_id", middle)));
        }

        DB configDB = getClient().getDB("config");
        DBCollection shards = configDB.getCollection("shards");
        DBCollection chunks = configDB.getCollection("chunks");
        
        long numChunks = chunks.count();
        Object chunk_source = chunks.findOne().get("shard");
        Object chunk_dest = shards.findOne(new BasicDBObject("_id", new BasicDBObject("$ne", chunk_source)));
        LOG.info("chunk source: " + chunk_source);
        LOG.info("chunk dest: " + chunk_dest);
        
        // shuffle chunks around
        for (int i = 0; i < numChunks / 2; i++) {
            DBObject chunk = chunks.findOne(new BasicDBObject("shard", chunk_source));
            LOG.info(String.format("moving %s from %s to %s", chunk,  chunk_source, chunk_dest));
            try {
                adminDB.command(new BasicDBObject("moveChunk", "mongo_hadoop.yield_historical.in")
                                    .append("find", chunk.get("min"))
                                    .append("to", chunk_dest));
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
    
    public MongoClient getClient2() {
        if(client2 == null) {
            try {
                client2 = new MongoClient("localhost:27018");
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return client2;
    }
    
}