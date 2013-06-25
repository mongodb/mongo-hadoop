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

}
