package com.mongodb.hadoop.util;

import com.mongodb.*;
import com.mongodb.hadoop.input.MongoInputSplit;
import java.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.*;


/* This implementation of MongoSplitter can be used for 
 * sharded systems. It treats each shard as a single split, by
 * reading all of the data on each shard for that split.
 * This should be used with caution as the input splits could
 * contain duplicate data from ongoing, or aborted migrations.
 */
public class ShardMongoSplitter extends MongoCollectionSplitter{

    private static final Log log = LogFactory.getLog( ShardMongoSplitter.class );

    public ShardMongoSplitter(Configuration conf, MongoURI inputURI){
        super(conf, inputURI);
    }

    // Treat each shard as one split.
    @Override
    public List<InputSplit> calculateSplits() throws SplitFailedException{
        this.init();
        final ArrayList<InputSplit> returnVal = new ArrayList<InputSplit>();

        DB configDB = this.mongo.getDB("config");
        DBCollection chunksCollection = configDB.getCollection( "chunks" );

        String inputNS = this.inputURI.getDatabase() + "." + this.inputURI.getCollection();

        Map<String, String> shardsMap = null;
        shardsMap = this.getShardsMap();

        for(Map.Entry<String,String> entry : shardsMap.entrySet()){
            String shardName = entry.getKey();
            String shardHosts = entry.getValue();

            MongoInputSplit chunkSplit = createSplitFromBounds((BasicDBObject)null, (BasicDBObject)null);
            MongoURI newURI = rewriteURI(inputURI, shardHosts);
            chunkSplit.setInputURI(newURI);
            returnVal.add(chunkSplit);
        }
        return returnVal;
    }

}
