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

package com.mongodb.hadoop.splitter;

import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Examines a collection and dynamically chooses which implementation of MongoSplitter to use, based on our configuration and the
 * collection's properties.
 */
public final class MongoSplitterFactory {

    private static final Log LOG = LogFactory.getLog(MongoSplitterFactory.class);

    private MongoSplitterFactory() {
    }

    public static MongoSplitter getSplitterByClass(final Configuration conf, final String className) {
        /* If user has specified a class to use for the splitter, use it */
        Class<? extends MongoSplitter> splitterClass =
            MongoConfigUtil.getClassByName(conf, className, MongoSplitter.class);
        if (splitterClass != null) {
            MongoSplitter splitter = ReflectionUtils.newInstance(splitterClass, conf);
            splitter.setConfiguration(conf);
            return splitter;
        } else {
            return null;
        }
    }

    public static MongoCollectionSplitter getSplitterByStats(final MongoClientURI uri, final Configuration config) {
        /* Looks at the collection in mongo.input.uri
         * and choose an implementation based on what's in there.  */

        MongoCollectionSplitter returnVal;

        // If the split calculation is totally disabled, just make one
        // big split for the whole collection.
        if (!MongoConfigUtil.createInputSplits(config)) {
            returnVal = new SingleMongoSplitter(config);
        } else {
            MongoClientURI authURI = MongoConfigUtil.getAuthURI(config);
            CommandResult stats;
            DBCollection coll;
            if (authURI != null) {
                coll = MongoConfigUtil.getCollectionWithAuth(uri, authURI);
                stats = coll.getStats();
                LOG.info("Retrieved Collection stats:" + stats);
            } else {
                coll = MongoConfigUtil.getCollection(uri);
                stats = coll.getStats();
            }

            if (!stats.getBoolean("ok", false)) {
                throw new RuntimeException("Unable to calculate input splits from collection stats: " + stats.getString("errmsg"));
            }

            if (!stats.getBoolean("sharded", false)) {
                returnVal = new StandaloneMongoSplitter(config);
            } else {
                // Collection is sharded
                if (MongoConfigUtil.isShardChunkedSplittingEnabled(config)) {
                    // Creates one split per chunk. 
                    returnVal = new ShardChunkMongoSplitter(config);
                } else if (MongoConfigUtil.canReadSplitsFromShards(config)) {
                    // Creates one split per shard, but ignores chunk bounds. 
                    // Reads from shards directly (bypassing mongos).
                    // Not usually recommended.
                    returnVal = new ShardMongoSplitter(config);
                } else {
                    //Not configured to use chunks or shards -
                    //so treat this the same as if it was an unsharded collection
                    returnVal = new StandaloneMongoSplitter(config);
                }
            }
        }
        return returnVal;
    }

    public static MongoSplitter getSplitter(final Configuration config) {
        String splitterClassName = config.get(MongoConfigUtil.MONGO_SPLITTER_CLASS);
        MongoSplitter customSplitter = getSplitterByClass(config, splitterClassName);
        if (customSplitter != null) {
            return customSplitter;
        } else {
            return getSplitterByStats(MongoConfigUtil.getInputURI(config), config);
        }
    }

}
