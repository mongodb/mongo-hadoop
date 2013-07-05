package com.mongodb.hadoop.util;

import com.mongodb.*;
import com.mongodb.hadoop.input.MongoInputSplit;
import java.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.*;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

/**
 *
 * Examines a collection and dynamically chooses which
 * implementation of MongoSplitter to use, based on our
 * configuration and the collection's properties.
 *
 */
public class MongoSplitterFactory{

    public static MongoSplitter getSplitter(Configuration config){

        // Split calculation is disabled, just make one big split
        // for the whole collection.
        if(!MongoConfigUtil.createInputSplits(config)){
            return new SingleMongoSplitter(config);
        }

        //Get the collection stats:
        MongoURI uri = MongoConfigUtil.getInputURI(config);
        DBCollection coll = MongoConfigUtil.getCollection(uri);
        DB db = coll.getDB(); 
        Mongo mongo = db.getMongo();

        if( MongoConfigUtil.getAuthURI(config) != null ){
            MongoURI authURI = MongoConfigUtil.getAuthURI(config);
            if(authURI.getUsername() != null &&
               authURI.getPassword() != null &&
               !authURI.getDatabase().equals(db.getName())) {
                DB authTargetDB = mongo.getDB(authURI.getDatabase());
                authTargetDB.authenticate(authURI.getUsername(),
                                          authURI.getPassword());
            }
        }

        final CommandResult stats = coll.getStats();
        final boolean isSharded = stats.getBoolean( "sharded", false );
        if(!isSharded){
            return new StandaloneMongoSplitter(config);
        }else{
            // Collection is sharded

            if(MongoConfigUtil.isShardChunkedSplittingEnabled(config)){
                // Creates one split per chunk. 
                return new ShardChunkMongoSplitter(config);
            }else if(MongoConfigUtil.canReadSplitsFromShards(config)){
                // Creates one split per shard, but ignoring chunk information. 
                // Reads from shards directly (bypassing mongos).
                // Not usually recommended.
                return new ShardMongoSplitter(config);
            }else{
                return new StandaloneMongoSplitter(config);
            }
        }
    }

}
