package com.mongodb.hadoop.util;

import com.mongodb.*;

public abstract class MongoSplitter{
    
    protected Configuration conf;
    protected Mongo mongo;

    public MongoSplitter(Configuration conf){
        this.conf = conf;
    }


    protected void init(){
        MongoURI inputURI = MongoConfigUtil.getInputURI(this.conf);
        DBCollection coll = MongoConfigUtil.getCollection(uri);
        DB db = coll.getDB(); 
        this.mongo = db.getMongo();
        if( MongoConfigUtil.getAuthURI(this.conf) != null ){
            MongoURI authURI = MongoConfigUtil.getAuthURI(conf);
            if(authURI.getUsername() != null &&
               authURI.getPassword() != null &&
               !authURI.getDatabase().equals(inputDB.getName())) {
                DB authTargetDB = inputMongo.getDB(authURI.getDatabase());
                authTargetDB.authenticate(authURI.getUsername(),
                                          authURI.getPassword());
            }
        }

    }
    
    public abstract List<InputSplit> calculateSplits();


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
                shardMap.put( (String) row.get( "_id" ), host );
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

}
