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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;


/* This class is an implementation of MongoSplitter which
 * calculates a list of splits on a single collection
 * by running the MongoDB internal command "splitVector",
 * which generates a list of index boundary pairs, each 
 * containing an approximate amount of data depending on the
 * max chunk size used, and converting those index boundaries
 * into splits.
 *
 * This splitter is the default implementation used for any
 * collection which is not sharded.
 *
 */
public class StandaloneMongoSplitter extends MongoCollectionSplitter {

    private static final Log LOG = LogFactory.getLog(StandaloneMongoSplitter.class);

    public StandaloneMongoSplitter() {
    }

    public StandaloneMongoSplitter(final Configuration conf) {
        super(conf);
    }

    @Override
    public List<InputSplit> calculateSplits() throws SplitFailedException {
        final DBObject splitKey = MongoConfigUtil.getInputSplitKey(getConfiguration());
        final int splitSize = MongoConfigUtil.getSplitSize(getConfiguration());
        final BasicDBObject splitMin = (BasicDBObject)MongoConfigUtil.getDBObject(getConfiguration(), MongoConfigUtil.SPLITS_MIN_KEY);
        final BasicDBObject splitMax = splitMin == null ? null : (BasicDBObject)MongoConfigUtil.getDBObject(getConfiguration(), MongoConfigUtil.SPLITS_MAX_KEY);
        final boolean splitKeyDescending = MongoConfigUtil.isSplitKeyDescending(getConfiguration());
        MongoClientURI inputURI;
        DBCollection inputCollection = null;
        final List<InputSplit> returnVal;
        try {
            inputURI = MongoConfigUtil.getInputURI(getConfiguration());
            inputCollection = MongoConfigUtil.getCollection(inputURI);

            //returnVal = new ArrayList<InputSplit>();
            final String ns = inputCollection.getFullName();

            LOG.info("Running splitvector to check splits against " + inputURI);
            final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start("splitVector", ns)
                                     .add("keyPattern", splitKey)
                                          // force:True is misbehaving it seems
                                     .add("force", false)
                                     .add("maxChunkSize", splitSize);
            if(splitMin != null) {
              builder.add( "min", splitMin );
              builder.add( "max", splitMax );
            }
            final DBObject cmd = builder.get();

            CommandResult data;
            boolean ok = true;
            if (authDB == null) {
                try {
                    data = inputCollection.getDB().getSisterDB("admin").command(cmd);
                } catch (MongoException e) {  // 2.0 servers throw exceptions rather than info in a CommandResult
                    data = null;
                    LOG.info(e.getMessage(), e);
                    if (e.getMessage().contains("unrecognized command: splitVector")) {
                        ok = false;
                    } else {
                        throw e;
                    }
                }
            } else {
                data = authDB.command(cmd);
            }

            if (data != null) {
                if (data.containsField("$err")) {
                    throw new SplitFailedException("Error calculating splits: " + data);
                } else if (!data.get("ok").equals(1.0)) {
                    ok = false;
                }
            }

            if (!ok) {
                CommandResult stats = inputCollection.getStats();
                if (stats.containsField("primary")) {
                    DBCursor shards = inputCollection.getDB().getSisterDB("config")
                                                     .getCollection("shards")
                                                     .find(new BasicDBObject("_id", stats.getString("primary")));
                    try {
                        if (shards.hasNext()) {
                            DBObject shard = shards.next();
                            String host = ((String) shard.get("host")).replace(shard.get("_id") + "/", "");
                            MongoClientURI shardHost = new MongoClientURIBuilder(inputURI)
                                                           .host(host)
                                                           .build();
                            MongoClient shardClient = null;
                            try {
                                shardClient = new MongoClient(shardHost);
                                data = shardClient.getDB("admin").command(cmd);
                            } catch (UnknownHostException e) {
                                LOG.error(e.getMessage(), e);
                            } finally {
                                if (shardClient != null) {
                                    shardClient.close();
                                }
                            }
                        }
                    } finally {
                        shards.close();
                    }
                }
                if (data != null && !data.get("ok").equals(1.0)) {
                    throw new SplitFailedException("Unable to calculate input splits: " + data.get("errmsg"));
                }

            }

            // Comes in a format where "min" and "max" are implicit
            // and each entry is just a boundary key; not ranged
            BasicDBList splitData = (BasicDBList) data.get("splitKeys");

            if (splitData.size() == 0) {
                LOG.warn("WARNING: No Input Splits were calculated by the split code. Proceeding with a *single* split. Data may be too"
                         + " small, try lowering 'mongo.input.split_size' if this is undesirable.");
            }
            returnVal = createSplits(splitData, splitMin, splitMax, splitKeyDescending);
        } finally {
            if (inputCollection != null) {
                MongoConfigUtil.close(inputCollection.getDB().getMongo());
            }
        }

        return returnVal;
    }

    protected List<InputSplit> createSplits( BasicDBList splitData, 
                    BasicDBObject splitMin, BasicDBObject splitMax, boolean descending ) throws SplitFailedException {
      final ArrayList<InputSplit> returnVal = new ArrayList<InputSplit>();

      BasicDBObject minKey = descending ? splitMax : splitMin;
      BasicDBObject maxKey = descending ? splitMin : splitMax;
      for( Object aSplitData : splitData ) {
        BasicDBObject currentKey = (BasicDBObject)aSplitData;
        returnVal.add(createSplitFromBounds(maxKey, currentKey));
        maxKey = currentKey;        
      }
      returnVal.add(createSplitFromBounds(maxKey,minKey));

      return returnVal; 
    }
}
