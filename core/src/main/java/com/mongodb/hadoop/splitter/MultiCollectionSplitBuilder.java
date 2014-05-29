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
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class MultiCollectionSplitBuilder {
    private LinkedList<CollectionSplitterConf> collectionSplitters;

    public MultiCollectionSplitBuilder() {
        collectionSplitters = new LinkedList<CollectionSplitterConf>();
    }

    public MultiCollectionSplitBuilder addConf(final CollectionSplitterConf conf) {
        collectionSplitters.add(conf);
        return this;
    }

    /**
     * @deprecated Use {@link #add(MongoClientURI, MongoClientURI, boolean, DBObject, DBObject, DBObject, boolean, Class)}
     */
    @Deprecated
    public MultiCollectionSplitBuilder add(final MongoURI inputURI, final MongoURI authURI, final boolean noTimeout, final DBObject fields,
                                           final DBObject sort, final DBObject query, final boolean useRangeQuery,
                                           final Class<? extends MongoSplitter> splitClass) {
        return add(new MongoClientURI(inputURI.toString()), new MongoClientURI(authURI.toString()),
                   noTimeout, fields, sort, query, useRangeQuery, splitClass);
    }

    public MultiCollectionSplitBuilder add(final MongoClientURI inputURI, final MongoClientURI authURI, final boolean noTimeout,
                                           final DBObject fields, final DBObject sort, final DBObject query, final boolean useRangeQuery,
                                           final Class<? extends MongoSplitter> splitClass) {
        return addConf(new CollectionSplitterConf(inputURI, authURI, noTimeout, fields, sort, query, useRangeQuery, splitClass));
    }

    public String toJSON() {
        BasicDBList returnVal = new BasicDBList();
        for (CollectionSplitterConf conf : collectionSplitters) {
            returnVal.add(new BasicDBObject(conf.toConfigMap()));
        }
        return JSON.serialize(returnVal);
    }

    public static class CollectionSplitterConf {
        private MongoClientURI inputURI = null;
        private MongoClientURI authURI = null;
        private boolean noTimeout = false;
        private DBObject fields = null;
        private DBObject sort = null;
        private DBObject query = null;
        private boolean useRangeQuery = false;
        private Class<? extends MongoSplitter> splitClass;

        public CollectionSplitterConf(final MongoClientURI inputURI, final MongoClientURI authURI, final boolean noTimeout,
                                      final DBObject fields, final DBObject sort, final DBObject query, final boolean useRangeQuery,
                                      final Class<? extends MongoSplitter> splitClass) {
            this.inputURI = inputURI;
            this.authURI = authURI;
            this.noTimeout = noTimeout;
            this.fields = fields;
            this.sort = sort;
            this.query = query;
            this.useRangeQuery = useRangeQuery;
            this.splitClass = splitClass;
        }

        public Map<String, String> toConfigMap() {
            HashMap<String, String> outMap = new HashMap<String, String>();

            if (inputURI != null) {
                outMap.put(MongoConfigUtil.INPUT_URI, inputURI.toString());
            }

            if (authURI != null) {
                outMap.put(MongoConfigUtil.AUTH_URI, authURI.toString());
            }

            outMap.put(MongoConfigUtil.INPUT_NOTIMEOUT, noTimeout ? "true" : "false");

            if (fields != null) {
                outMap.put(MongoConfigUtil.INPUT_FIELDS, JSON.serialize(fields));
            }

            if (sort != null) {
                outMap.put(MongoConfigUtil.INPUT_SORT, JSON.serialize(sort));
            }

            if (query != null) {
                outMap.put(MongoConfigUtil.INPUT_QUERY, JSON.serialize(query));
            }

            outMap.put(MongoConfigUtil.SPLITS_USE_RANGEQUERY, useRangeQuery ? "true" : "false");

            if (splitClass != null) {
                outMap.put(MongoConfigUtil.MONGO_SPLITTER_CLASS, splitClass.getCanonicalName());
            }

            return outMap;
        }
    }

}
