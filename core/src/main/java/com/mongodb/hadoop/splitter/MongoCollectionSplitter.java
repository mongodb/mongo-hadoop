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
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.BSONObject;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MongoCollectionSplitter extends MongoSplitter {

    private static final Log LOG = LogFactory.getLog(MongoCollectionSplitter.class);

    public static final MinKey MIN_KEY_TYPE = new MinKey();
    public static final MaxKey MAX_KEY_TYPE = new MaxKey();
    //CHECKSTYLE:OFF
    protected Mongo mongo;
    protected DBCollection inputCollection;

    //A reference to the database that we authenticated against, if there was
    //some authURI provided in the config.
    protected DB authDB;
    //CHECKSTYLE:ON

    /*
    protected MongoURI inputURI;
    protected MongoURI authURI;
    protected DBObject query;
    protected boolean useRangeQuery;
    protected boolean noTimeout;
    protected DBObject fields;
    protected DBObject sort;*/

    public MongoCollectionSplitter() {
    }

    public MongoCollectionSplitter(final Configuration conf) {
        super(conf);
    }
    //
    //     public void setInputURI(MongoURI inputURI){
    //         this.inputURI = inputURI;
    //     }
    //
    //     public void setQuery(DBObject query){
    //         this.query = query;
    //     }
    //
    //     public BSONObject getQuery(){
    //         return this.query;
    //     }
    //
    //     public boolean getUseRangeQuery(){
    //         return this.useRangeQuery;
    //     }
    //
    //     public void setUseRangeQuery(boolean useRangeQuery){
    //         this.useRangeQuery = useRangeQuery;
    //     }
    //
    //     public void setAuthURI(MongoURI authURI){
    //         this.authURI = authURI;
    //     }
    //
    //     public boolean getNoTimeout(){
    //         return this.noTimeout;
    //     }
    //
    //     public void setNoTimeout(boolean noTimeout){
    //         this.noTimeout = noTimeout;
    //     }
    //
    //     public MongoURI getAuthURI(){
    //         return this.authURI;
    //     }
    //
    //     public DBObject getFields(){
    //         return this.fields;
    //     }
    //
    //     public void setFields(DBObject fields){
    //         this.fields = fields;
    //     }
    //
    //     public DBObject getSort(){
    //         return this.sort;
    //     }
    //
    //     public void setSort(DBObject sort){
    //         this.sort = sort;
    //     }

    protected void init() {
        MongoURI inputURI = MongoConfigUtil.getInputURI(conf);
        
        DB db;
        try {
            this.inputCollection = MongoConfigUtil.getCollection(inputURI);
            db = this.inputCollection.getDB();
        } catch (Exception e) {
            String message = e.getMessage() + "\n\nMongo connection strings are required to be of the form:\n" +
                    " mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]]/database.collection";
            throw new IllegalStateException(message, e);
        }

        this.mongo = db.getMongo();
        MongoURI authURI = MongoConfigUtil.getAuthURI(conf);
        if (authURI != null) {
            if (authURI.getUsername() != null
                && authURI.getPassword() != null
                && !mongo.getDB(authURI.getDatabase()).isAuthenticated()) {
                DB authTargetDB = mongo.getDB(authURI.getDatabase());
                authDB = authTargetDB;
                authTargetDB.authenticate(authURI.getUsername(), authURI.getPassword());
            }
        }
    }

    @Override
    public abstract List<InputSplit> calculateSplits() throws SplitFailedException;

    /**
     * Contacts the config server and builds a map of each shard's name to its host(s) by examining config.shards.
     */
    protected Map<String, String> getShardsMap() {
        DB configDB = this.mongo.getDB("config");
        final HashMap<String, String> shardsMap = new HashMap<String, String>();
        DBCollection shardsCollection = configDB.getCollection("shards");
        DBCursor cur = shardsCollection.find();
        try {
            while (cur.hasNext()) {
                final BasicDBObject row = (BasicDBObject) cur.next();
                String host = row.getString("host");
                // for replica sets host will look like: "setname/localhost:20003,localhost:20004"
                int slashIndex = host.indexOf('/');
                if (slashIndex > 0) {
                    host = host.substring(slashIndex + 1);
                }
                shardsMap.put((String) row.get("_id"), host);
            }
        } finally {
            cur.close();
        }
        return shardsMap;
    }

    /**
     * Takes an existing {@link MongoURI} and returns a new modified URI which replaces the original's server host + port with a supplied
     * new server host + port, but maintaining all the same original options. This is useful for generating distinct URIs for each mongos
     * instance so that large batch reads can all target them separately, distributing the load more evenly. It can also be used to force a
     * block of data to be read directly from the shard's servers directly, bypassing mongos entirely.
     *
     * @param originalUri  the URI to rewrite
     * @param newServerUri the new host(s) to target, e.g. server1:port1[,server2:port2,...]
     */
    protected static MongoURI rewriteURI(final MongoURI originalUri, final String newServerUri) {
        String originalUriString = originalUri.toString();
        originalUriString = originalUriString.substring(MongoURI.MONGODB_PREFIX.length());

        // uris look like: mongodb://fred:foobar@server1[,server2]/path?options
        //

        //Locate the last character of the original hostname
        int serverEnd;
        int idx = originalUriString.lastIndexOf("/");
        serverEnd = idx < 0 ? originalUriString.length() : idx;

        //Locate the first character of the original hostname
        idx = originalUriString.indexOf("@");
        int serverStart = idx > 0 ? idx + 1 : 0;

        StringBuilder sb = new StringBuilder(originalUriString);
        sb.replace(serverStart, serverEnd, newServerUri);
        String ans = MongoURI.MONGODB_PREFIX + sb.toString();
        return new MongoURI(ans);
    }

    /**
     * Create an instance of MongoInputSplit that represents a view of this splitter's input URI between the given lower/upper bounds. If
     * this splitter has range queries enabled, it will attempt to use $gt/$lte clauses in the query construct to create the split,
     * otherwise it will use min/max index boundaries (default behavior)
     *
     * @param lowerBound the lower bound of the collection
     * @param upperBound the upper bound of the collection
     */
    public MongoInputSplit createSplitFromBounds(final BasicDBObject lowerBound, final BasicDBObject upperBound)
        throws SplitFailedException {
        LOG.info("Created split: min=" + (lowerBound != null ? lowerBound.toString() : "null") + ", max= " + (upperBound != null
                                                                                                              ? upperBound.toString()
                                                                                                              : "null"));
        //Objects to contain upper/lower bounds for each split
        DBObject splitMin = new BasicDBObject();
        DBObject splitMax = new BasicDBObject();
        if (lowerBound != null) {
            for (Map.Entry<String, Object> entry : lowerBound.entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();

                if (!val.equals(MIN_KEY_TYPE)) {
                    splitMin.put(key, val);
                }
            }
        }

        if (upperBound != null) {
            for (Map.Entry<String, Object> entry : upperBound.entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();

                if (!val.equals(MAX_KEY_TYPE)) {
                    splitMax.put(key, val);
                }
            }
        }

        MongoInputSplit split = null;

        //If enabled, attempt to build the split using $gt/$lte
        DBObject query = MongoConfigUtil.getQuery(conf);
        if (MongoConfigUtil.isRangeQueryEnabled(conf)) {
            try {
                query = MongoConfigUtil.getQuery(conf);
                split = createRangeQuerySplit(lowerBound, upperBound, query);
            } catch (Exception e) {
                throw new SplitFailedException("Couldn't use range query to create split: " + e.getMessage());
            }
        }
        if (split == null) {
            split = new MongoInputSplit();
            BasicDBObject splitQuery = new BasicDBObject();
            if (query != null) {
                splitQuery.putAll(query);
            }
            split.setQuery(splitQuery);
            split.setMin(splitMin);
            split.setMax(splitMax);
        }
        split.setInputURI(MongoConfigUtil.getInputURI(conf));
        split.setAuthURI(MongoConfigUtil.getAuthURI(conf));
        split.setNoTimeout(MongoConfigUtil.isNoTimeout(conf));
        split.setFields(MongoConfigUtil.getFields(conf));
        split.setSort(MongoConfigUtil.getSort(conf));
        return split;
    }

    /**
     * Creates an instance of {@link MongoInputSplit} whose upper and lower bounds are restricted by adding $gt/$lte clauses to the query
     * filter. This requires that the boundaries are not compound keys, and that the query does not contain any keys used in the split key.
     *
     * @param chunkLowerBound the lower bound of the chunk (min)
     * @param chunkUpperBound the upper bound of the chunk (max)
     * @throws IllegalArgumentException if the query conflicts with the chunk bounds, or the either of the bounds are compound keys.
     */
    public MongoInputSplit createRangeQuerySplit(final BasicDBObject chunkLowerBound, final BasicDBObject chunkUpperBound,
                                                 final BSONObject query) {

        //If the boundaries are actually empty, just return
        //a split without boundaries.
        if (chunkLowerBound == null && chunkUpperBound == null) {
            DBObject splitQuery = new BasicDBObject();
            splitQuery.putAll(query);
            MongoInputSplit split = new MongoInputSplit();
            split.setQuery(splitQuery);
            return split;
        }

        //The boundaries are not empty, so try to build
        //a split using $gt/$lte.

        //First check that the split contains no compound keys.
        // e.g. this is valid: { _id : "foo" }
        // but this is not {_id : "foo", name : "bar"}
        Map.Entry<String, Object> minKey = chunkLowerBound != null && chunkLowerBound.keySet().size() == 1
                                           ? chunkLowerBound.entrySet().iterator().next() : null;
        Map.Entry<String, Object> maxKey = chunkUpperBound != null && chunkUpperBound.keySet().size() == 1
                                           ? chunkUpperBound.entrySet().iterator().next() : null;
        if (minKey == null && maxKey == null) {
            throw new IllegalArgumentException("Range query is enabled but one or more split boundaries contains a compound key:\n"
                                               + "min:  " + chunkLowerBound + "\nmax:  " + chunkUpperBound);
        }

        //Now, check that the lower and upper bounds don't have any keys
        //which overlap with the query.
        if ((minKey != null && query.containsKey(minKey.getKey()))
            || (maxKey != null && query.containsKey(maxKey.getKey()))) {
            throw new IllegalArgumentException("Range query is enabled but split key conflicts with query filter:\n"
                                               + "min:  " + chunkLowerBound + "\nmax:  " + chunkUpperBound + "\nquery:  " + query);
        }

        String key = null;
        BasicDBObject rangeObj = new BasicDBObject();
        if (minKey != null) {
            key = minKey.getKey();
            rangeObj.put("$gte", minKey.getValue());
        }

        if (maxKey != null) {
            key = maxKey.getKey();
            rangeObj.put("$lt", maxKey.getValue());
        }

        DBObject splitQuery = new BasicDBObject();
        splitQuery.putAll(query);
        splitQuery.put(key, rangeObj);
        MongoInputSplit split = new MongoInputSplit();
        split.setQuery(splitQuery);
        return split;
    }

}
