/*
 * Copyright 2010-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * Examines a collection and dynamically chooses which
 * implementation of MongoSplitter to use, based on our
 * configuration and the collection's properties.
 *
 */
public class MongoSplitterFactory{

    public static MongoSplitter getSplitter(Configuration config){

        /* If user has specified a class to use for the splitter, use it */
        Class<? extends MongoSplitter> splitterClass = MongoConfigUtil.getSplitterClass(config);
        if(splitterClass != null){
            MongoSplitter splitter = (MongoSplitter)ReflectionUtils.newInstance(splitterClass, config);
            return splitter;
        }

        /* Otherwise, just look at the collection in mongo.input.uri
         * and choose an implementation based on what's in there.  */

        MongoCollectionSplitter returnVal;

        MongoURI uri = MongoConfigUtil.getInputURI(config);

        // Split calculation is disabled, just make one big split
        // for the whole collection.
        if(!MongoConfigUtil.createInputSplits(config)){
            returnVal = new SingleMongoSplitter(config, uri);
        }else{
            //Get the collection stats
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
                final int splitSize = MongoConfigUtil.getSplitSize(config);
                final DBObject splitKey = MongoConfigUtil.getInputSplitKey(config);
                returnVal = new StandaloneMongoSplitter(config,
                                                        uri,
                                                        splitKey,
                                                        splitSize);
            }else{
                // Collection is sharded
                if(MongoConfigUtil.isShardChunkedSplittingEnabled(config)){
                    // Creates one split per chunk. 
                    boolean targetShards = MongoConfigUtil.canReadSplitsFromShards(config);
                    returnVal = new ShardChunkMongoSplitter(config,
                                                            uri,
                                                            targetShards);
                }else if(MongoConfigUtil.canReadSplitsFromShards(config)){
                    // Creates one split per shard, but ignoring chunk information. 
                    // Reads from shards directly (bypassing mongos).
                    // Not usually recommended.
                    returnVal = new ShardMongoSplitter(config, uri);
                }else{
                    //Not configured to use chunks or shards -
                    //so treat this the same as if it was an unsharded collection
                    final int splitSize = MongoConfigUtil.getSplitSize(config);
                    final DBObject splitKey = MongoConfigUtil.getInputSplitKey(config);
                    returnVal = new StandaloneMongoSplitter(config,
                                                            uri,
                                                            splitKey,
                                                            splitSize);
                }
            }
        }
        returnVal.setAuthURI(MongoConfigUtil.getAuthURI(config));
        returnVal.setQuery(MongoConfigUtil.getQuery(config));
        returnVal.setUseRangeQuery(MongoConfigUtil.isRangeQueryEnabled(config));
        returnVal.setNoTimeout(MongoConfigUtil.isNoTimeout(config));
        returnVal.setFields(MongoConfigUtil.getFields(config));
        returnVal.setSort(MongoConfigUtil.getSort(config));
        return returnVal;
    }

}
