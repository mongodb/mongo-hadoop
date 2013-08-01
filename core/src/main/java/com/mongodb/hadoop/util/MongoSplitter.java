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

import java.util.*;
import com.mongodb.*;
import com.mongodb.hadoop.input.*;
import org.bson.*;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.*;

public abstract class MongoSplitter {

    protected Configuration conf;

    public MongoSplitter(){ }

    public MongoSplitter(Configuration conf){
        this.conf = conf;
    }

    public void setConfiguration(Configuration conf){
        this.conf = conf;
    }

    public abstract List<InputSplit> calculateSplits()
        throws SplitFailedException;

    public static class MongoSplitterOptions {
        MongoURI authURI;
        boolean splittingEnabled = true;
        int splitSize = MongoConfigUtil.DEFAULT_SPLIT_SIZE;
        DBObject splitKey = new BasicDBObject("_id", 1);
        boolean targetShards = false;
        boolean useChunks = true;
        DBObject query = null;
        boolean useRangeQuery = false;
        boolean notimeout = false;
        DBObject fields = null;
        DBObject sort = null;
        Class splitterClass = null;

        public MongoSplitterOptions(Configuration config){
            this.authURI = MongoConfigUtil.getAuthURI(config);
            this.splittingEnabled = MongoConfigUtil.createInputSplits(config);
            this.splitSize = MongoConfigUtil.getSplitSize(config);
            this.splitKey = MongoConfigUtil.getInputSplitKey(config);
            this.targetShards = MongoConfigUtil.canReadSplitsFromShards(config);
            this.useChunks = MongoConfigUtil.isShardChunkedSplittingEnabled(config);
            this.useRangeQuery = MongoConfigUtil.isRangeQueryEnabled(config);
            this.query = MongoConfigUtil.getQuery(config);
            this.notimeout = MongoConfigUtil.isNoTimeout(config);
            this.fields = MongoConfigUtil.getFields(config);
            this.sort = MongoConfigUtil.getSort(config);  
            this.splitterClass = MongoConfigUtil.getSplitterClass(config);
        }

        public MongoSplitterOptions(Map<String,Object> configOptions,
                Configuration conf){
            if(configOptions.containsKey("authURI"))
                this.authURI = new MongoURI((String)configOptions.get("authURI"));
            if(configOptions.containsKey("splittingEnabled"))
                this.splittingEnabled = (Boolean)configOptions.get("splittingEnabled");
            if(configOptions.containsKey("splitSize"))
                this.splitSize = (Integer)configOptions.get("splitSize");
            if(configOptions.containsKey("splitKey"))
                this.splitKey = new BasicDBObject((Map)configOptions.get("splitKey"));
            if(configOptions.containsKey("targetShards"))
                this.targetShards = (Boolean)configOptions.get("targetShards");
            if(configOptions.containsKey("useChunks"))
                this.useChunks = (Boolean)configOptions.get("useChunks");
            if(configOptions.containsKey("query"))
                this.query = new BasicDBObject((Map)configOptions.get("query"));
            if(configOptions.containsKey("notimeout"))
                this.notimeout = (Boolean)configOptions.get("notimeout");
            if(configOptions.containsKey("fields"))
                this.fields = new BasicDBObject((Map)configOptions.get("fields"));
            if(configOptions.containsKey("sort"))
                this.sort = new BasicDBObject((Map)configOptions.get("sort"));

            //Get the class name. If the key isn't in the map, null is OK.
            if(configOptions.containsKey("splitterClassName")){
                String className = (String)configOptions.get("splitterClassName");
                this.splitterClass = MongoConfigUtil.getClassByName(conf,
                                                        className, 
                                                        MongoCollectionSplitter.class);
                //The class name is either bogus, or doesn't subclass MongoSplitter
                if(this.splitterClass == null){
                    throw new IllegalArgumentException("splitterClassName " + 
                            "must refer to a class that extends " +
                            "com.mongodb.hadoop.util.MongoSplitter");
                }
            }
        }
    }

}
