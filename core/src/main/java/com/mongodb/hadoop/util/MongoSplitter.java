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

public abstract class MongoSplitter{

    private static final Log log = LogFactory.getLog( MongoSplitter.class );
    
    public static final MinKey MIN_KEY_TYPE = new MinKey();
    public static final MaxKey MAX_KEY_TYPE = new MaxKey();
    protected Configuration conf;
    protected Mongo mongo;
    protected MongoURI inputURI;
    protected DBCollection inputCollection;

    public MongoSplitter(Configuration conf){
        this.conf = conf;
    }


    protected void init(){
        this.inputURI = MongoConfigUtil.getInputURI(this.conf);
        this.inputCollection = MongoConfigUtil.getCollection(inputURI);
        DB db = this.inputCollection.getDB(); 
        this.mongo = db.getMongo();
        if( MongoConfigUtil.getAuthURI(this.conf) != null ){
            MongoURI authURI = MongoConfigUtil.getAuthURI(conf);
            if(authURI.getUsername() != null &&
               authURI.getPassword() != null &&
               !authURI.getDatabase().equals(db.getName())) {
                DB authTargetDB = this.mongo.getDB(authURI.getDatabase());
                authTargetDB.authenticate(authURI.getUsername(),
                                          authURI.getPassword());
            }
        }
    }


    public abstract List<InputSplit> calculateSplits() throws SplitFailedException;

    /**
     *  Contacts the config server and builds a map of each shard's name
     *  to its host(s) by examining config.shards.
     *
     */
    protected Map<String, String> getShardsMap(){
        DB configDB = this.mongo.getDB("config");
        final HashMap<String, String> shardsMap = new HashMap<String,String>();
        DBCollection shardsCollection = configDB.getCollection( "shards" );
        DBCursor cur = shardsCollection.find();
        try {
            while ( cur.hasNext() ){
                final BasicDBObject row = (BasicDBObject) cur.next();
                String host = row.getString( "host" );
                // for replica sets host will look like: "setname/localhost:20003,localhost:20004"
                int slashIndex = host.indexOf( '/' );
                if ( slashIndex > 0 )
                    host = host.substring( slashIndex + 1 );
                shardsMap.put( (String) row.get( "_id" ), host );
            }
        } finally {
            if ( cur != null )
                cur.close();
        }
        return shardsMap;
    }

    /**
     *  Takes an existing {@link MongoURI} and returns a new modified URI which
     *  replaces the original's server host + port with a supplied new 
     *  server host + port, but maintaining all the same original options.
     *  This is useful for generating distinct URIs for each mongos instance
     *  so that large batch reads can all target them separately, distributing the
     *  load more evenly. It can also be used to force a block of data to be read
     *  directly from the shard's servers directly, bypassing mongos entirely.
     *
     * @param originalUri the URI to rewrite
     * @param newServerUri the new host(s) to target, e.g. server1:port1[,server2:port2,...]
     *
     */
    protected static MongoURI rewriteURI( MongoURI originalUri, String newServerUri){
        String originalUriString = originalUri.toString();
        originalUriString = originalUriString.substring( MongoURI.MONGODB_PREFIX.length() );

        // uris look like: mongodb://fred:foobar@server1[,server2]/path?options
        //

        //Locate the last character of the original hostname
        int serverEnd;
        int idx = originalUriString.lastIndexOf( "/" );
        serverEnd = idx < 0 ? originalUriString.length() : idx;

        //Locate the first character of the original hostname
        idx = originalUriString.indexOf( "@" );
        int serverStart = idx > 0 ? idx + 1 : 0;

        StringBuilder sb = new StringBuilder( originalUriString );
        sb.replace( serverStart, serverEnd, newServerUri );
        String ans = MongoURI.MONGODB_PREFIX + sb.toString();
        return new MongoURI( ans );
    }

    public MongoInputSplit createSplitFromBounds(BasicDBObject lowerBound, BasicDBObject upperBound) throws SplitFailedException{
        //Objects to contain upper/lower bounds for each split
        DBObject splitMin = new BasicDBObject();
        DBObject splitMax = new BasicDBObject();
        if(lowerBound != null){
            for( Map.Entry<String,Object> entry : lowerBound.entrySet() ){
                String key = entry.getKey();
                Object val = entry.getValue();

                if(!val.equals(MIN_KEY_TYPE))
                    splitMin.put(key, val);
                if(upperBound != null){
                    Object maxVal = upperBound.get(key);
                    if(!val.equals(MAX_KEY_TYPE))
                        splitMax.put(key, maxVal);
                }
            }
        }


        BSONObject query = MongoConfigUtil.getQuery(this.conf);
        MongoInputSplit split = null;
        //If enabled, attempt to build the split using $gt/$lte
        if(MongoConfigUtil.isRangeQueryEnabled(this.conf)){
            try{
                split = createRangeQuerySplit(lowerBound, upperBound, query);
            }catch(Exception e){
                throw new SplitFailedException("Couldn't use range query to create split: " + e.getMessage());
            }
        }
        if(split == null){
            split = new MongoInputSplit();
            BasicDBObject splitQuery = new BasicDBObject();
            splitQuery.putAll(query);
            split.setQuery(splitQuery);
            split.setMin(splitMin);
            split.setMax(splitMax);
        }
        split.setInputURI(MongoConfigUtil.getInputURI(this.conf));
        split.setNoTimeout(MongoConfigUtil.isNoTimeout(this.conf));
        split.setFields(MongoConfigUtil.getFields(this.conf));
        split.setSort(MongoConfigUtil.getSort(this.conf));
        return split;
    }

    /**
     *  Creates an instance of {@link MongoInputSplit} whose upper and lower
     *  bounds are restricted by adding $gt/$lte clauses to the query filter. 
     *  This requires that the boundaries are not compound keys, and that
     *  the query does not contain any keys used in the split key.
     *
     * @param chunkLowerBound the lower bound of the chunk (min)
     * @param chunkUpperBound the upper bound of the chunk (max)
     * @throws IllegalArgumentException if the query conflicts with the chunk
     * bounds, or the either of the bounds are compound keys.
     *
     */
    public MongoInputSplit createRangeQuerySplit(BasicDBObject chunkLowerBound, BasicDBObject chunkUpperBound, BSONObject query){

        //If the boundaries are actually empty, just return
        //a split without boundaries.
        if(chunkLowerBound == null && chunkUpperBound == null){
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
        Map.Entry<String, Object> minKey = chunkLowerBound != null && chunkLowerBound.keySet().size() == 1 ?
            chunkLowerBound.entrySet().iterator().next() : null;
        Map.Entry<String, Object> maxKey = chunkUpperBound != null && chunkUpperBound.keySet().size() == 1 ?
            chunkUpperBound.entrySet().iterator().next() : null;
        if(minKey == null && maxKey == null ){
            throw new IllegalArgumentException("Range query is enabled but one or more split boundaries contains a compound key:\n" +
                      "min:  " + chunkLowerBound +  "\n" +
                      "max:  " + chunkUpperBound);
        }

        //Now, check that the lower and upper bounds don't have any keys
        //which overlap with the query.
        if( (minKey != null && query.containsKey(minKey.getKey())) ||
            (maxKey != null && query.containsKey(maxKey.getKey())) ){
            throw new IllegalArgumentException("Range query is enabled but split key conflicts with query filter:\n" +
                      "min:  " + chunkLowerBound +  "\n" +
                      "max:  " + chunkUpperBound +  "\n" + 
                      "query:  " + query);
        }

        BasicDBObject rangeObj = new BasicDBObject();
        if( minKey!=null )
            rangeObj.put("$gte", minKey.getValue());

        if( maxKey!=null )
            rangeObj.put("$lt", maxKey.getValue());

        DBObject splitQuery = new BasicDBObject();
        splitQuery.putAll(query);
        splitQuery.put(minKey.getKey(), rangeObj);
        MongoInputSplit split = new MongoInputSplit();
        split.setQuery(splitQuery);
        return split;
    }

}
