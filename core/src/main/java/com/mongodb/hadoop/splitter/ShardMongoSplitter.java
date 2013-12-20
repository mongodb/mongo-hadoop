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

import com.mongodb.MongoURI;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * This implementation of MongoSplitter can be used for sharded systems. It treats each shard as a single split, by reading all of the data
 * on each shard for that split. This should be used with caution as the input splits could contain duplicate data from ongoing, or aborted
 * migrations.
 */
public class ShardMongoSplitter extends MongoCollectionSplitter {

    public ShardMongoSplitter(final Configuration conf) {
        super(conf);
    }

    // Treat each shard as one split.
    @Override
    public List<InputSplit> calculateSplits() throws SplitFailedException {
        this.init();
        final ArrayList<InputSplit> returnVal = new ArrayList<InputSplit>();

        MongoURI inputURI = MongoConfigUtil.getInputURI(conf);

        Map<String, String> shardsMap;
        shardsMap = this.getShardsMap();

        for (Map.Entry<String, String> entry : shardsMap.entrySet()) {
            String shardHosts = entry.getValue();

            MongoInputSplit chunkSplit = createSplitFromBounds(null, null);
            MongoURI newURI = rewriteURI(inputURI, shardHosts);
            chunkSplit.setInputURI(newURI);
            returnVal.add(chunkSplit);
        }
        return returnVal;
    }

}
