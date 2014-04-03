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
import com.mongodb.DBCursor;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is an implementation of MongoSplitter which can be used on sharded collections. It gets the chunks information from the
 * cluster's config server, and produces one split for each chunk.
 */
public class ShardChunkMongoSplitter extends MongoCollectionSplitter {

    private static final Log LOG = LogFactory.getLog(ShardChunkMongoSplitter.class);

    public ShardChunkMongoSplitter(final Configuration conf) {
        super(conf);
    }

    // Generate one split per chunk.
    @Override
    public List<InputSplit> calculateSplits() throws SplitFailedException {
        this.init();
        boolean targetShards = MongoConfigUtil.canReadSplitsFromShards(conf);
        DB configDB = this.mongo.getDB("config");
        DBCollection chunksCollection = configDB.getCollection("chunks");

        MongoURI inputURI = MongoConfigUtil.getInputURI(conf);
        String inputNS = inputURI.getDatabase() + "." + inputURI.getCollection();

        DBCursor cur = chunksCollection.find(new BasicDBObject("ns", inputNS));

        int numChunks = 0;

        Map<String, String> shardsMap = null;
        if (targetShards) {
            try {
                shardsMap = this.getShardsMap();
            } catch (Exception e) {
                //Something went wrong when trying to
                //read the shards data from the config server,
                //so abort the splitting
                throw new SplitFailedException("Couldn't get shards information from config server", e);
            }
        }

        List<String> mongosHostNames = MongoConfigUtil.getInputMongosHosts(this.conf);
        if (targetShards && mongosHostNames.size() > 0) {
            throw new SplitFailedException("Setting both mongo.input.split.read_from_shards and mongo.input.mongos_hosts"
                                           + " does not make sense. ");
        }

        if (mongosHostNames.size() > 0) {
            LOG.info("Using multiple mongos instances (round robin) for reading input.");
        }

        Map<String, LinkedList<InputSplit>> shardToSplits = new HashMap<String, LinkedList<InputSplit>>();

        while (cur.hasNext()) {
            final BasicDBObject row = (BasicDBObject) cur.next();
            BasicDBObject chunkLowerBound = (BasicDBObject) row.get("min");
            BasicDBObject chunkUpperBound = (BasicDBObject) row.get("max");
            MongoInputSplit chunkSplit = createSplitFromBounds(chunkLowerBound, chunkUpperBound);
            chunkSplit.setInputURI(inputURI);
            String shard = (String) row.get("shard");
            if (targetShards) {
                //The job is configured to target shards, so replace the
                //mongos hostname with the host of the shard's servers
                String shardHosts = shardsMap.get(shard);
                if (shardHosts == null) {
                    throw new SplitFailedException("Couldn't find shard ID: " + shard + " in config.shards.");
                }

                MongoURI newURI = rewriteURI(inputURI, shardHosts);
                chunkSplit.setInputURI(newURI);
            } else if (mongosHostNames.size() > 0) {
                //Multiple mongos hosts are specified, so
                //choose a host name in round-robin fashion
                //and rewrite the URI using that hostname.
                //This evenly distributes the load to avoid
                //pegging a single mongos instance.
                String roundRobinHost = mongosHostNames.get(numChunks % mongosHostNames.size());
                MongoURI newURI = rewriteURI(inputURI, roundRobinHost);
                chunkSplit.setInputURI(newURI);
            }
            LinkedList<InputSplit> shardList = shardToSplits.get(shard);
            if (shardList == null) {
                shardList = new LinkedList<InputSplit>();
                shardToSplits.put(shard, shardList);
            }
            shardList.add(chunkSplit);
            numChunks++;
        }

        return createSplitList(numChunks, shardToSplits);
    }

    /**
     * Round robin splits across shards.  The splits are going to end up as Map jobs
     * processed in the same order as the splits.  We want to have continuous map
     * jobs be on separate shards so that as you're completing map jobs the work
     * is spread evenly across shard machines.
     * 
     * @param numChunks - Number of chunks
     * @param shardToSplits - Map of shardName to list of splits on that shard.
     */
    protected static List<InputSplit> createSplitList(int numChunks,
            Map<String, LinkedList<InputSplit>> shardToSplits) {
        final List<InputSplit> splits = new ArrayList<InputSplit>(numChunks);
        int splitIndex = 0;
        while (splitIndex < numChunks) {
            Set<String> shardSplitsToRemove = new HashSet<String>();
            for (Map.Entry<String, LinkedList<InputSplit>> shardSplits : shardToSplits.entrySet()) {
                LinkedList<InputSplit> shardSplitsList = shardSplits.getValue();
                InputSplit split = shardSplitsList.pop();
                splits.add(splitIndex, split);
                splitIndex++;
                if (shardSplitsList.isEmpty()) {
                    shardSplitsToRemove.add(shardSplits.getKey());
                }
            }
            for (String shardName : shardSplitsToRemove) {
                shardToSplits.remove(shardName);
            }
        }

        return splits;
    }

}
