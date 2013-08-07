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

import com.mongodb.*;
import com.mongodb.util.*;
import com.mongodb.hadoop.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import java.util.*;

public class MultiCollectionSplitBuilder{
    LinkedList<CollectionSplitterConf> collectionSplitters;

    public MultiCollectionSplitBuilder(){
        collectionSplitters = new LinkedList<CollectionSplitterConf>();
    }

    public MultiCollectionSplitBuilder addConf(CollectionSplitterConf conf){
        collectionSplitters.add(conf);
        return this;
    }

    public MultiCollectionSplitBuilder add(MongoURI inputURI,
                                           MongoURI authURI,
                                           boolean notimeout,
                                           DBObject fields,
                                           DBObject sort,
                                           DBObject query,
                                           boolean useRangeQuery, 
                                           Class<? extends MongoSplitter> splitClass){
        CollectionSplitterConf conf = new CollectionSplitterConf();
        conf.inputURI = inputURI;
        conf.authURI = authURI;
        conf.notimeout = notimeout;
        conf.fields = fields;
        conf.sort = sort;
        conf.query = query;
        conf.useRangeQuery = useRangeQuery;
        conf.splitClass = splitClass;
        return addConf(conf);
    }

    public String toJSON(){
        BasicDBList returnVal = new BasicDBList();
        for(CollectionSplitterConf conf : this.collectionSplitters){
            returnVal.add(new BasicDBObject(conf.toConfigMap()));
        }
        return JSON.serialize(returnVal);
    }

    public static class CollectionSplitterConf{
        MongoURI inputURI = null;
        MongoURI authURI = null;
        boolean notimeout = false;
        DBObject fields = null;
        DBObject sort = null;
        DBObject query = null;
        boolean useRangeQuery = false;
        Class<? extends MongoSplitter> splitClass;

        public Map<String,String> toConfigMap(){
            HashMap<String,String> outMap = new HashMap<String,String>();

            if(inputURI != null)
                outMap.put(MongoConfigUtil.INPUT_URI, inputURI.toString());

            if(authURI != null)
                outMap.put(MongoConfigUtil.AUTH_URI, authURI.toString());

            outMap.put(MongoConfigUtil.INPUT_NOTIMEOUT, notimeout ? "true" : "false");

            if(fields != null)
                outMap.put(MongoConfigUtil.INPUT_FIELDS, JSON.serialize(fields));

            if(sort != null)
                outMap.put(MongoConfigUtil.INPUT_SORT, JSON.serialize(sort));

            if(query != null)
                outMap.put(MongoConfigUtil.INPUT_QUERY, JSON.serialize(query));

            outMap.put(MongoConfigUtil.SPLITS_USE_RANGEQUERY, useRangeQuery ? "true" : "false");

            if(splitClass != null){
                outMap.put(MongoConfigUtil.MONGO_SPLITTER_CLASS, splitClass.getCanonicalName());
            }

            return outMap;
        }
    }

}
