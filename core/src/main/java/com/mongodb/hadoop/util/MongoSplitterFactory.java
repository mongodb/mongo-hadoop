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
import java.net.UnknownHostException;
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

    private static final Log log = LogFactory.getLog( MongoSplitterFactory.class );

    public static MongoSplitter getSplitterByClass(Configuration conf, String className){
        /* If user has specified a class to use for the splitter, use it */
        Class<? extends MongoSplitter> splitterClass =
            MongoConfigUtil.getClassByName(conf, className, MongoSplitter.class);
        if(splitterClass != null){
            MongoSplitter splitter = (MongoSplitter)ReflectionUtils.newInstance(splitterClass, conf);
            splitter.setConfiguration(conf);
            return splitter;
        }else{
            return null;
        }
    }

    public static MongoCollectionSplitter getSplitterByStats(MongoURI uri,
            MongoSplitter.MongoSplitterOptions options, Configuration config){
        /* Looks at the collection in mongo.input.uri
         * and choose an implementation based on what's in there.  */

        MongoCollectionSplitter returnVal;

        // Split calculation is disabled, just make one big split
        // for the whole collection.
        if(!options.splittingEnabled){
            returnVal = new SingleMongoSplitter(config, uri);
        }else{
            MongoURI statsTargetURI;
            MongoURI authURI = options.authURI;
            CommandResult stats;
            DBCollection coll;
            if(authURI != null){
                coll = MongoConfigUtil.getCollectionWithAuth(uri, authURI);
                stats = coll.getStats();
                log.info("Retrieved Collection stats:" + stats);
            }else{
                coll = MongoConfigUtil.getCollection(uri);
                stats = coll.getStats();
            }

            final boolean isSharded = stats.getBoolean( "sharded", false );
            if(!isSharded){
                final int splitSize = options.splitSize;
                final DBObject splitKey = options.splitKey;
                returnVal = new StandaloneMongoSplitter(config,
                                                        uri,
                                                        splitKey,
                                                        splitSize);
            }else{
                // Collection is sharded
                if(options.useChunks){
                    // Creates one split per chunk. 
                    returnVal = new ShardChunkMongoSplitter(config, uri, options.targetShards);
                }else if(options.targetShards){
                    // Creates one split per shard, but ignoring chunk information. 
                    // Reads from shards directly (bypassing mongos).
                    // Not usually recommended.
                    returnVal = new ShardMongoSplitter(config, uri);
                }else{
                    //Not configured to use chunks or shards -
                    //so treat this the same as if it was an unsharded collection
                    returnVal = new StandaloneMongoSplitter(config,
                                                            uri,
                                                            options.splitKey,
                                                            options.splitSize);
                }
            }
        }
        returnVal.setAuthURI(options.authURI);
        returnVal.setQuery(options.query);
        returnVal.setUseRangeQuery(options.useRangeQuery);
        returnVal.setNoTimeout(options.notimeout);
        returnVal.setFields(options.fields);
        returnVal.setSort(options.sort);
        return returnVal;
    }

    public static MongoSplitter getSplitter(Configuration config){
        String splitterClassName = config.get(MongoConfigUtil.MONGO_SPLITTER_CLASS);
        MongoSplitter customSplitter = getSplitterByClass(config, splitterClassName);
        if(customSplitter != null){
            return customSplitter;
        } else {
            MongoURI inputURI = MongoConfigUtil.getInputURI(config);
            MongoSplitter.MongoSplitterOptions splitOptions = 
                new MongoSplitter.MongoSplitterOptions(config);
            return getSplitterByStats(inputURI, splitOptions, config);
        }
    }

}
