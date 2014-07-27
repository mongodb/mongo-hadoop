package com.mongodb.hadoop;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assume.assumeTrue;

public class BaseShardedTest extends TreasuryTest {
    private static final Log LOG = LogFactory.getLog(BaseShardedTest.class);
    private MongoClient shard1;
    private MongoClient shard2;
    private MongoClient mongos;

    @Before
    public void shuffleChunks() throws IOException, InterruptedException, TimeoutException {
        assumeTrue(isSharded(getInputUri()));
        LOG.info("shuffling chunks across shards");

        DB adminDB = getClient(getInputUri()).getDB("admin");
        adminDB.command(new BasicDBObject("enablesharding", "mongo_hadoop"));

        getClient(getInputUri()).getDB("config").getCollection("settings").update(new BasicDBObject("_id", "balancer"),
                                                                     new BasicDBObject("$set", new BasicDBObject("stopped", true)),
                                                                     false,
                                                                     true);
        adminDB.command(new BasicDBObject("shardCollection", "mongo_hadoop.yield_historical.in")
                            .append("key", new BasicDBObject("_id", 1)));

        DBCollection historicalIn = getClient(getInputUri()).getDB("mongo_hadoop").getCollection("yield_historical.in");

        for (int chunkpos : new int[]{2000, 3000, 1000, 500, 4000, 750, 250, 100, 3500, 2500, 2250, 1750}) {
            Object middle = historicalIn.find().sort(new BasicDBObject("_id", 1)).skip(chunkpos).iterator().next().get("_id");
            adminDB.command(new BasicDBObject("split", "mongo_hadoop.yield_historical.in")
                                .append("middle", new BasicDBObject("_id", middle)));
        }

        DB configDB = getMongos().getDB("config");
        DBCollection shards = configDB.getCollection("shards");
        DBCollection chunks = configDB.getCollection("chunks");

        long numChunks = chunks.count();
        Object chunkSource = chunks.findOne().get("shard");
        Object chunkDest = shards.findOne(new BasicDBObject("_id", new BasicDBObject("$ne", chunkSource)));
        LOG.info("chunk source: " + chunkSource);
        LOG.info("chunk dest: " + chunkDest);

        // shuffle chunks around
        for (int i = 0; i < numChunks / 2; i++) {
            DBObject chunk = chunks.findOne(new BasicDBObject("shard", chunkSource));
            LOG.info(String.format("moving %s from %s to %s", chunk, chunkSource, chunkDest));
            try {
                adminDB.command(new BasicDBObject("moveChunk", "mongo_hadoop.yield_historical.in")
                                    .append("find", chunk.get("min"))
                                    .append("to", chunkDest));
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public MongoClient getMongos() {
        if (mongos == null) {
            try {
                mongos = new MongoClient(getInputUri());
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return mongos;
    }

    public MongoClient getMongos2() {
        if (mongos == null) {
            try {
                mongos = new MongoClient(new MongoClientURIBuilder(getInputUri()).port(27018).build());
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return mongos;
    }

    public MongoClient getShard1() {
        if (shard2 == null) {
            try {
                shard2 = new MongoClient(authCheck(new MongoClientURIBuilder().port(27217)).build());
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return shard2;
    }

    public MongoClient getShard2() {
        if (shard2 == null) {
            try {
                shard2 = new MongoClient(authCheck(new MongoClientURIBuilder().port(27218)).build());
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return shard2;
    }

}