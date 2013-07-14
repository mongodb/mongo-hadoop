package com.mongodb.hadoop.util;

import com.mongodb.*;
import com.mongodb.hadoop.input.MongoInputSplit;
import java.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.*;


public class ShardChunkMongoSplitter extends MongoCollectionSplitter{

    private static final Log log = LogFactory.getLog( ShardChunkMongoSplitter.class );
    
    protected boolean targetShards = false;

    public ShardChunkMongoSplitter(Configuration conf, MongoURI inputURI, boolean targetShards){
        super(conf, inputURI);
        this.targetShards = targetShards;
    }

    // Generate one split per chunk.
    @Override
    public List<InputSplit> calculateSplits() throws SplitFailedException{
        this.init();
        DB configDB = this.mongo.getDB("config");
        DBCollection chunksCollection = configDB.getCollection( "chunks" );

        String inputNS = this.inputURI.getDatabase() + "." + this.inputURI.getCollection();

        DBCursor cur = chunksCollection.find(new BasicDBObject("ns", inputNS));

        int numChunks = 0;

        Map<String, String> shardsMap = null;
        if(this.targetShards){
            shardsMap = this.getShardsMap();
        }

        ArrayList<InputSplit> returnVal = new ArrayList<InputSplit>();

        while(cur.hasNext()){
            numChunks++;
            final BasicDBObject row = (BasicDBObject)cur.next();
            BasicDBObject chunkLowerBound = (BasicDBObject)row.get("min");
            BasicDBObject chunkUpperBound = (BasicDBObject)row.get("max");
            MongoInputSplit chunkSplit = createSplitFromBounds(chunkLowerBound, chunkUpperBound);
            if(shardsMap != null){
                //The job is configured to target shards, so replace the
                //mongos hostname with the host of the shard's servers
                String shard = (String)row.get("shard");
                String shardHosts = shardsMap.get(shard);
                if(shardHosts == null)
                    throw new SplitFailedException("Couldn't find shard ID: " + shard + " in config.shards.");

                MongoURI newURI = rewriteURI(inputURI, shardHosts);
                chunkSplit.setInputURI(newURI);
            }else{
                chunkSplit.setInputURI(inputURI);
            }
            returnVal.add(chunkSplit);
        }
        return returnVal;
    }

}
