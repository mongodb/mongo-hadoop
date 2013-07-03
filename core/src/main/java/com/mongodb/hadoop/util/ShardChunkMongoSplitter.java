package com.mongodb.hadoop.util;

import com.mongodb.*;
import org.bson.types.MinKey;
import org.bson.types.MaxKey;


public class ShardChunkMongoSplitter{

    private static final MinKey MIN_KEY_TYPE = new MinKey();
    private static final MaxKey MAX_KEY_TYPE = new MaxKey();

    // Generate one split per chunk.
    @Override
    public List<InputSplit> calculateSplits() throws SplitFailedException{
        this.init();
        DB configDB = this.mongo.getDB("config");
        DBCollection chunksCollection = configDB.getCollection( "chunks" );

        MongoURI inputURI = MongoConfigUtil.getInputURI(this.conf);
        String inputNS = inputURI.getDatabase() + "." + inputURI.getCollection();

        DBCursor cur = chunksCollection.find(new BasicDBObject("ns", inputNS));

        int numChunks = 0;

        boolean useShards = MongoConfigUtil.canReadSplitsFromShards(this.conf);
        Map<String, String> shardsMap = null;
        if(useShards){
            shardsMap = this.getShardsMap();
        }

        while(cur.hasNext()){
            numChunks++;
            final BasicDBObject row = (BasicDBObject) cur.next();
            BSONObject chunkLowerBound = (BSONObject)row.get("min");
            BSONObject chunkUpperBound = (BSONObject)row.get("max");
            MongoInputSplit chunkSplit = createSplitFromChunk(row);
            if(shardsMap != null){
                //The job is configured to target shards, so replace the
                //mongos hostname with the host of the shard's servers
                String shard = (String)row.get("shard");
                String shardHosts = shardsMap.get(shard);
                if(shardHosts == null)
                    throw new SplitFailedException("Couldn't find shard ID: " + shard + " in config.shards.");

                MongoURI newURI = rewriteURI(newURI, shardHosts);
                chunkSplit.setInputURI(newURI);
            }else{
                chunkSplit.setInputURI(inputURI);
            }
        }

    }

    protected MongoInputSplit createSplitFromChunk(BasicDBObject chunk) throws SplitFailedException{
        //Lower and upper bounds according to chunk on config server
        BSONObject chunkLowerBound = (BSONObject)row.get("min");
        BSONObject chunkUpperBound = (BSONObject)row.get("max");

        //Objects to contain upper/lower bounds for each split
        BSONObject splitMin = new BasicBSONObject();
        BSONObject splitMax = new BasicBSONObject();
        for( Map.Entry<String,Object> entry : chunkLowerBound.entrySet() ){
            String key = entry.getKey();
            Object val = entry.getValue();
            Object maxVal = chunkUpperBound.get(key);

            if(!val.equals(MIN_KEY_TYPE))
                splitMin.put(key, val);
            if(!val.equals(MAX_KEY_TYPE))
                splitMax.put(key, maxVal);
        }


        BSONObject query = MongoConfigUtil.getQuery(this.conf);
        MongoInputSplit split = null;
        //If enabled, attempt to build the split using $gt/$lte
        if(MongoConfigUtil.isRangeQueryEnabled()){
            try{
                split = createRangeQuerySplit(chunkLowerBound, chunkUpperBound, query);
            }catch(Exception e){
                log.error("Couldn't use range query to create split: " + e.getMessage());
                throw new SplitFailedException("Couldn't use range query to create split: " + e.getMessage());
            }
        }
        if(split == null){
            split = new MongoInputSplit();
            BasicDBObject splitQuery = new BasicDBObject();
            splitQuery.addAll(query);
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
    private MongoInputSplit createRangeQuerySplit(BSONObject chunkLowerBound, BSONObject chunkUpperBound, BSONObject query){
        //First check that the split contains no compound keys.
        // e.g. this is valid: { _id : "foo" }
        // but this is not {_id : "foo", name : "bar"}
        Map.Entry<String, Object> minKey = chunkLowerBound.size() == 1 ?
            min.entrySet().iterator().next() : null;
        Map.Entry<String, Object> maxKey = chunkUpperBound.size() == 1 ?
            max.entrySet().iterator().next() : null;
        if(minKey == null && maxKey == null ){
            throw new IllegalArgumentException("Range query is enabled but one or more split boundaries contains a compound key:\n" +
                      "minKey:  " + chunkLowerBound.toString() +  "\n" +
                      "maxKey:  " + chunkUpperBound.toString());
        }

        //Now, check that the lower and upper bounds don't have any keys
        //which overlap with the query.
        if( (minKey != null && originalQuery.containsKey(minKey.getKey())) ||
            (maxKey != null && originalQuery.containsKey(maxKey.getKey())) ){
            throw new IllegalArgumentException("Range query is enabled but split key conflicts with query filter:\n" +
                      "minKey:  " + chunkLowerBound.toString() +  "\n" +
                      "maxKey:  " + chunkUpperBound.toString() +  "\n" + 
                      "query:  " + originalQuery.toString());
        }

        BasicDBObject rangeObj = new BasicDBObject();
        if( minKey!=null )
            rangeObj.put("$gte", minKey.getValue());

        if( maxKey!=null )
            rangeObj.put("$lt", maxKey.getValue());

        splitQuery = new BasicDBObject();
        splitQuery.putAll(query);
        splitQuery.put(minKey.getKey(), rangeObj);
        MongoInputSplit split = new MongoInputSplit();
        split.setQuery(splitQuery);
    }

}
