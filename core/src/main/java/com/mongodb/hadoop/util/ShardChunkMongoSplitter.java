public class ShardChunkMongoSplitter{

    private static final MinKey MIN_KEY_TYPE = new MinKey();
    private static final MaxKey MAX_KEY_TYPE = new MaxKey();

    // Generate one split per chunk.
    @Override
    public List<InputSplit> calculateSplits(){
        this.init();
        DB configDB = this.mongo.getDB("config");
        DBCollection chunksCollection = configDB.getCollection( "chunks" );

        MongoURI inputURI = MongoConfigUtil.getInputURI(this.conf);
        String inputNS = inputURI.getDatabase() + "." + inputURI.getCollection();

        DBCursor cur = chunksCollection.find(new BasicDBObject("ns", inputNS));

        int numChunks = 0;
        while(cur.hasNext()){
            numChunks++;
            final BasicDBObject row = (BasicDBObject) cur.next();

            BSONObject chunkLowerBound = (BSONObject)row.get("min");
            BSONObject chunkUpperBound = (BSONObject)row.get("max");


                for ( String keyName : minObj.keySet() ){
                    Object tMin = minObj.get( keyName );
                    Object tMax = ( (DBObject) row.get( "max" ) ).get( keyName );
                    /** The shard key can be of any possible type, so this must be kept as Object */
                    if ( !( tMin == SplitFriendlyDBCallback.MIN_KEY_TYPE || tMin.equals( "MinKey" ) ) )
                        min.put( keyName, tMin );
                    if ( !( tMax == SplitFriendlyDBCallback.MAX_KEY_TYPE || tMax.equals( "MaxKey" ) ) )
                        max.put( keyName, tMax );
                }

        }
            int numChunks = 0;
            final int numExpectedChunks = cur.size();

            final List<InputSplit> splits = new ArrayList<InputSplit>( numExpectedChunks );

    }

    protected MongoInputSplit createSplitFromChunk(BasicDBObject chunk){
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

        BasicBSONObject query = new BasicBSONObject();

        //If enabled, attempt to build the split using $gt/$lte
        if(MongoConfigUtil.isRangeQueryEnabled()){
            //Check that the split contains no compound keys.
            // e.g. this is valid: { _id : "foo" }
            // but this is not {_id : "foo", name : "bar"}
            Map.Entry<String, Object> minKey = min.size() == 1 ?
                min.entrySet().iterator().next() : null;
            Map.Entry<String, Object> maxKey = max.size() == 1 ?
                max.entrySet().iterator().next() : null;
            if(minKey == null && maxKey == null ){
                throw new IllegalArgumentException("Range query is enabled but one or more split boundaries contains a compound key:\n" +
                          "minKey:  " + min.toString() +  "\n" +
                          "maxKey:  " + max.toString());
            }

            //Now, check that the lower and upper bounds don't have any keys
            //which overlap with the 
        }

    }


}
