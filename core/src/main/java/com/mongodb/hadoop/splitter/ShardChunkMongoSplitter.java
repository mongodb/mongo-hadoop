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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is an implementation of MongoSplitter which can be used on sharded collections. It gets the chunks information from the
 * cluster's config server, and produces one split for each chunk.
 */
public class ShardChunkMongoSplitter extends MongoCollectionSplitter {

    private static final Log LOG = LogFactory.getLog(ShardChunkMongoSplitter.class);

    public ShardChunkMongoSplitter() {
    }

    public ShardChunkMongoSplitter(final Configuration conf) {
        super(conf);
    }

    /**
     * Get a list of InputSplits based on a list of MongoDB shard chunks, the shard key, and a
     * mapping of shard names to host names. This is used internally by {@link #calculateSplits()}.
     *
     * @param chunks Chunk documents from the config.chunks collection.
     * @param shardsMap A map of shard name -> an array of hostnames.
     * @return A list of InputSplits.
     */
    List<InputSplit> calculateSplitsFromChunks(
      final List<DBObject> chunks, final Map<String, List<String>> shardsMap)
      throws SplitFailedException {

        boolean targetShards = MongoConfigUtil.canReadSplitsFromShards(getConfiguration());
        List<String> mongosHostNames = MongoConfigUtil.getInputMongosHosts(getConfiguration());
        MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
        if (targetShards && mongosHostNames.size() > 0) {
            throw new SplitFailedException("Setting both mongo.input.split.read_from_shards and mongo.input.mongos_hosts"
              + " does not make sense. ");
        }
        Map<String, String> mongosMap = null;
        if (mongosHostNames.size() > 0) {
            // Build a map of host -> mongos host string (incl. port)
            mongosMap = new HashMap<String, String>();
            for (String mongosHostName : mongosHostNames) {
                String[] hostAndPort = mongosHostName.split(":");
                mongosMap.put(hostAndPort[0], mongosHostName);
            }
        }

        List<InputSplit> splits = new ArrayList<InputSplit>(chunks.size());
        for (DBObject chunk : chunks) {
            BasicDBObject chunkLowerBound = (BasicDBObject) chunk.get("min");
            BasicDBObject chunkUpperBound = (BasicDBObject) chunk.get("max");
            MongoInputSplit chunkSplit = createSplitFromBounds(chunkLowerBound, chunkUpperBound);
            chunkSplit.setInputURI(inputURI);
            String shard = (String) chunk.get("shard");
            if (targetShards) {
                //The job is configured to target shards, so replace the
                //mongos hostname with the host of the shard's servers
                List<String> shardHosts = shardsMap.get(shard);
                if (shardHosts == null) {
                    throw new SplitFailedException(
                      "Couldn't find shard ID: " + shard + " in config.shards.");
                }

                MongoClientURI newURI = rewriteURI(inputURI, shardHosts);
                chunkSplit.setInputURI(newURI);
            } else if (mongosMap != null) {
                // Try to use a mongos collocated with one of the shard hosts for the input
                // split. If the user has their Hadoop/MongoDB clusters configured correctly,
                // this will allow for reading without having to transfer data over a network.
                // Note that MongoInputSplit.getLocations() just returns the hostnames from its
                // input URI.
                List<String> chunkHosts = shardsMap.get(shard);
                String mongosHost = null;
                for (String chunkHost : chunkHosts) {
                    String[] hostAndPort = chunkHost.split(":");
                    mongosHost = mongosMap.get(hostAndPort[0]);
                    if (mongosHost != null) {
                        break;
                    }
                }
                if (null == mongosHost) {
                    // Fall back just to using the given input URI.
                    chunkSplit.setInputURI(inputURI);
                } else {
                    LOG.info("Will read split " + chunkSplit + " from mongos " + mongosHost);
                    chunkSplit.setInputURI(rewriteURI(inputURI, mongosHost));
                }
            }
            // Add this split to the list for the current shard.
            chunkSplit.setKeyField(MongoConfigUtil.getInputKey(getConfiguration()));
            splits.add(chunkSplit);
        }

        if (MongoConfigUtil.isFilterEmptySplitsEnabled(getConfiguration())) {
            return filterEmptySplits(splits);
        }
        return splits;
    }

    // Generate one split per chunk.
    @Override
    public List<InputSplit> calculateSplits() throws SplitFailedException {
        DB configDB = getConfigDB();
        DBCollection chunksCollection = configDB.getCollection("chunks");
        Map<String, List<String>> shardsMap;
        try {
            shardsMap = getShardsMap();
        } catch (Exception e) {
            //Something went wrong when trying to
            //read the shards data from the config server,
            //so abort the splitting
            throw new SplitFailedException("Couldn't get shards information from config server", e);
        }

        return calculateSplitsFromChunks(chunksCollection.find().toArray(), shardsMap);
    }

}
