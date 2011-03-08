// MongoImportFormat.java
/*
 * Copyright 2010 10gen Inc.
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

package com.mongodb.hadoop;

import com.mongodb.CommandResult;
import com.mongodb.MongoURI;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;

import com.mongodb.hadoop.input.*;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.bson.types.ObjectId;

public class MongoInputFormat extends InputFormat<Object, BSONObject> {
    private static final Log log = LogFactory.getLog( MongoInputFormat.class );

    public RecordReader<Object, BSONObject> createRecordReader( InputSplit split , TaskAttemptContext context ){
        if ( !( split instanceof MongoInputSplit ) )
            throw new IllegalStateException( "Creation of a new RecordReader requires a MongoInputSplit instance." );

        final MongoInputSplit mis = (MongoInputSplit) split;

        return new MongoRecordReader( mis );
    }

    public List<InputSplit> getSplits( JobContext context ){
        final Configuration hadoop_configuration = context.getConfiguration();
        final MongoConfig conf = new MongoConfig( hadoop_configuration);

        if ( conf.getLimit() > 0 || conf.getSkip() > 0 )
            /**
             * TODO - If they specify skip or limit we create only one input
             * split
             */
            throw new IllegalArgumentException( "skip() and limit() is not currently supported do to input split issues." );
        else {
            /**
             * On the jobclient side we want *ONLY* the min and max ids for each
             * split; Actual querying will be done on the individual mappers.
             */
            boolean useShards = hadoop_configuration.getBoolean(MongoConfigUtil.SPLITS_USE_SHARDS , false);
            //use_chunks should be true if use_shards is true unless overridden
            boolean useChunks = hadoop_configuration.getBoolean(MongoConfigUtil.SPLITS_USE_CHUNKS , useShards);
            List<InputSplit> splits = null;
            if(useShards || useChunks){
                try{
                    com.mongodb.MongoURI uri = conf.getInputURI();
                    com.mongodb.Mongo mongo = uri.connect();
                    com.mongodb.DB db = mongo.getDB(uri.getDatabase());
                    com.mongodb.DBCollection coll = db.getCollection(uri.getCollection());
                    final CommandResult stats = coll.getStats();
                    final boolean isSharded = stats.getBoolean("sharded", false);
                    if (isSharded) { //don't proceed if the collection isn't actually sharded
                        if (useChunks)
                            splits = getSplitsUsingChunks(conf, uri, mongo, useShards);
                        else if(useShards)
                            splits = getSplitsUsingShards(conf, uri, mongo);
                    }
                    mongo.close();
                    if (splits != null)
                        return splits;
                    //If splits is null fall through to code below
                }catch  (Exception e) {
                    log.error("Could not get splits (use_shards: "+useShards+", use_chunks: "+useChunks+")", e);
                    e.printStackTrace();
                }
            }
            // Number of *documents*, not bytes, to split on
            final int splitSize = conf.getSplitSize();
            splits = new ArrayList<InputSplit>(1);
            // no splits, no sharding
            splits.add( new MongoInputSplit( conf.getInputURI() , conf.getQuery() , conf.getFields() , conf.getSort() , conf.getLimit() , conf.getSkip() ) );
            
            log.info( "Calculated " + splits.size() + " split objects." );
            return splits;
        }
    }
    /** This gets the URIs to the backend {@code mongod}s and
     * returns splits that connect directly to those backends.
     * The main problem with this is that clients that can connect to {@code mongos}
     * can't necessarily connect to the individual {@code mongod}s.
     * There also might be concurrency issues (if chunks are in the process of
     * getting moved around). */
    private List<InputSplit> getSplitsUsingShards( final MongoConfig conf,
             com.mongodb.MongoURI uri, com.mongodb.Mongo mongo) {
        log.warn("WARNING getting splits using shards w/o chunks is risky and might not produce correct results");
        com.mongodb.DB config_db = mongo.getDB("config");
        com.mongodb.DBCollection shards_coll = config_db.getCollection("shards");

        java.util.Set<String> shard_set = new java.util.HashSet<String>();
        
        com.mongodb.DBCursor cur = shards_coll.find();
        while (cur.hasNext()) {
            final com.mongodb.DBObject row = cur.next();
            //System.out.println(row);
            shard_set.add((String) row.get("host"));
        }
        final List<InputSplit> splits = new ArrayList<InputSplit>( shard_set.size() );
        //todo: using stats only get the shards that actually host data for this collection
        for (String host : shard_set) {
            com.mongodb.MongoURI this_uri = getNewURI(uri, host);
            splits.add(new MongoInputSplit(this_uri, conf.getQuery(), conf.getFields(), conf.getSort(), conf.getLimit(), conf.getSkip()));
        }
        return splits;
    }
    /** This constructs splits by compounding the original query with queries that
     * limit the input of an individual split to only one backend.
     * This doesn't work for some reason. Delete before release. */
    private boolean sharding_second_pass(List<InputSplit> splits, final MongoConfig conf,
            com.mongodb.MongoURI uri, com.mongodb.Mongo mongo) {
        com.mongodb.DBObject orig_query = conf.getQuery();
        Map orig_query_map = null;
        if (orig_query != null){
            orig_query_map = Collections.unmodifiableMap(orig_query.toMap());
        }
        com.mongodb.DB config_db = mongo.getDB("config");
        com.mongodb.DBCollection chunks_coll = config_db.getCollection("chunks");
/*Chunks looks like:
{ "_id" : "test.lines-_id_ObjectId('4d60b839874a8ad69ad8adf6')", "lastmod" : { "t" : 3000, "i" : 1 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b839874a8ad69ad8adf6") }, "max" : { "_id" : ObjectId("4d60b83a874a8ad69ad8d1a9") }, "shard" : "shard0000" }
{ "_id" : "test.lines-_id_ObjectId('4d60b83a874a8ad69ad8d1a9')", "lastmod" : { "t" : 4000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b83a874a8ad69ad8d1a9") }, "max" : { "_id" : ObjectId("4d60b83d874a8ad69ad8fb7c") }, "shard" : "shard0000" }
{ "_id" : "test.lines-_id_ObjectId('4d60b83d874a8ad69ad8fb7c')", "lastmod" : { "t" : 5000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b83d874a8ad69ad8fb7c") }, "max" : { "_id" : ObjectId("4d60b83e874a8ad69ad928e8") }, "shard" : "shard0001" }
{ "_id" : "test.lines-_id_ObjectId('4d60b83e874a8ad69ad928e8')", "lastmod" : { "t" : 6000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b83e874a8ad69ad928e8") }, "max" : { "_id" : ObjectId("4d60b840874a8ad69ad95853") }, "shard" : "shard0000" }
{ "_id" : "test.lines-_id_ObjectId('4d60b840874a8ad69ad95853')", "lastmod" : { "t" : 7000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b840874a8ad69ad95853") }, "max" : { "_id" : ObjectId("4d60b842874a8ad69ad985db") }, "shard" : "shard0001" }
{ "_id" : "test.lines-_id_ObjectId('4d60b842874a8ad69ad985db')", "lastmod" : { "t" : 8000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b842874a8ad69ad985db") }, "max" : { "_id" : ObjectId("4d60b843874a8ad69ad9b4c2") }, "shard" : "shard0000" }
{ "_id" : "test.lines-_id_ObjectId('4d60b843874a8ad69ad9b4c2')", "lastmod" : { "t" : 9000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b843874a8ad69ad9b4c2") }, "max" : { "_id" : ObjectId("4d60b844874a8ad69ad9e750") }, "shard" : "shard0001" }
{ "_id" : "test.lines-_id_ObjectId('4d60b844874a8ad69ad9e750')", "lastmod" : { "t" : 9000, "i" : 1 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b844874a8ad69ad9e750") }, "max" : { "_id" : ObjectId("4d60b845874a8ad69ada1c71") }, "shard" : "shard0002" }
{ "_id" : "test.lines-_id_ObjectId('4d60b845874a8ad69ada1c71')", "lastmod" : { "t" : 3000, "i" : 16 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b845874a8ad69ada1c71") }, "max" : { "_id" : ObjectId("4d60b846874a8ad69ada4720") }, "shard" : "shard0002" }
{ "_id" : "test.lines-_id_ObjectId('4d60b846874a8ad69ada4720')", "lastmod" : { "t" : 3000, "i" : 18 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b846874a8ad69ada4720") }, "max" : { "_id" : ObjectId("4d60b848874a8ad69ada8756") }, "shard" : "shard0002" }
{ "_id" : "test.lines-_id_ObjectId('4d60b848874a8ad69ada8756')", "lastmod" : { "t" : 3000, "i" : 19 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b848874a8ad69ada8756") }, "max" : { "_id" : { $maxKey : 1 } }, "shard" : "shard0002" }
*/
         com.mongodb.BasicDBObject query = new com.mongodb.BasicDBObject();
        query.put("ns", uri.getDatabase()+"."+uri.getCollection());

        com.mongodb.DBCursor cur = chunks_coll.find(query);
        com.mongodb.BasicDBObject sort = new com.mongodb.BasicDBObject();
        sort.append("min", new com.mongodb.BasicDBObject(Collections.singletonMap("_id", 1)));
        cur.sort(sort);

        String last_shard_name = null;
        Object saved_min_val = null; // _id can be of any type
        Object last_max_val = null;
        int num_chunks = 0;
        while (cur.hasNext()) {
            num_chunks++;
            final com.mongodb.DBObject row = cur.next();
            String this_shard_name = (String) row.get("shard");
            //System.out.println("row " + num_chunks + ") " + row + " shard is: " + this_shard_name);
            if (!this_shard_name.equals(last_shard_name)) {
                com.mongodb.DBObject min_obj = ((com.mongodb.DBObject) row.get("min"));
                String keyname = min_obj.keySet().iterator().next();
                //org.bson.types.ObjectId this_min_val = (ObjectId) min_obj.get(keyname);
                Object this_min_val = min_obj.get(keyname);
                System.out.println("this_min_val is a "+this_min_val.getClass().getName());
                String this_min_val_str = this_min_val.toString();
                //                org.bson.types.ObjectId this_max_val = ((DBObject) row.get("max")).get(keyname);
                //                this_max_val.
                //String this_max_val = ((DBObject)row.get("max")).get(keyname).toString();
                if (last_shard_name != null){ //this is not the first iteration
                    Map id_query_map = new HashMap();
                    if ( ! (saved_min_val instanceof String))
                        id_query_map.put("$gte",  saved_min_val);
                    if (! (this_min_val instanceof String))
                        id_query_map.put("$lt", this_min_val);
                    com.mongodb.BasicDBObject new_query = new com.mongodb.BasicDBObject(orig_query_map);
                    new_query.put("_id",id_query_map); //todo: check that original query didn't have _id.  If it did merge the queries
                    System.out.println(" new_query is: "+new_query);
                    splits.add( new MongoInputSplit( conf.getInputURI() , new_query , conf.getFields() , conf.getSort() , conf.getLimit() , conf
                                                     .getSkip() ) );
                    ///
                }
                saved_min_val = this_min_val;
                last_shard_name = this_shard_name;
                last_max_val =  (ObjectId) ((com.mongodb.DBObject) row.get("max")).get(keyname);

            }
        }
        if (last_max_val != null){ //if there was one iteration
            Map id_query_map = new HashMap();
            if ( ! (saved_min_val instanceof String))
                id_query_map.put("$gte",  saved_min_val);
            if ( ! (last_max_val instanceof String))
                id_query_map.put("$lt",  last_max_val);


            com.mongodb.BasicDBObject new_query = new com.mongodb.BasicDBObject(orig_query_map);
            new_query.put("_id",id_query_map);
            System.out.println(" last new_query is: "+new_query);
            splits.add( new MongoInputSplit( conf.getInputURI() , new_query , conf.getFields() , conf.getSort() , conf.getLimit() , conf
                                             .getSkip() ) );
        }
        System.out.println(" there were "+num_chunks+" chunks returning "+splits.size()+" splits");
        return true;
    }
     /** This constructs splits using the chunk boundries.
      <p>In future iterations this will detect if the shard backend is part of a
      * replica set. If so it will round robin the chunks between members of the replica set. */
    private List<InputSplit> getSplitsUsingChunks(final MongoConfig conf,
            com.mongodb.MongoURI uri, com.mongodb.Mongo mongo, boolean useShards) {
        com.mongodb.DBObject originalQuery = conf.getQuery();
        Map origQueryMap = null;
        if (originalQuery != null){
            origQueryMap = Collections.unmodifiableMap(originalQuery.toMap());
        }
        com.mongodb.DB configDB = mongo.getDB("config");
        Map<String, String> shardMap = null; //key: shardname, value: host
        if (useShards){
            shardMap = new HashMap<String, String>();
            com.mongodb.DBCollection shards_coll = configDB.getCollection("shards");
            com.mongodb.DBCursor cur = shards_coll.find();
            while (cur.hasNext()) {
                final com.mongodb.DBObject row = cur.next();
                //System.out.println(row);
                shardMap.put((String) row.get("_id"), (String) row.get("host"));
                //todo: for replicate sets trim off the replica prefix
                //  Get slave_ok from conf property. If not set there then
                //get from original url. Set slaveok in new uris
            }
            cur.close();
        }
        com.mongodb.DBCollection chunksCollection = configDB.getCollection("chunks");
/* Chunks looks like:
{ "_id" : "test.lines-_id_ObjectId('4d60b839874a8ad69ad8adf6')", "lastmod" : { "t" : 3000, "i" : 1 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b839874a8ad69ad8adf6") }, "max" : { "_id" : ObjectId("4d60b83a874a8ad69ad8d1a9") }, "shard" : "shard0000" }
{ "_id" : "test.lines-_id_ObjectId('4d60b83a874a8ad69ad8d1a9')", "lastmod" : { "t" : 4000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b83a874a8ad69ad8d1a9") }, "max" : { "_id" : ObjectId("4d60b83d874a8ad69ad8fb7c") }, "shard" : "shard0000" }
{ "_id" : "test.lines-_id_ObjectId('4d60b83d874a8ad69ad8fb7c')", "lastmod" : { "t" : 5000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b83d874a8ad69ad8fb7c") }, "max" : { "_id" : ObjectId("4d60b83e874a8ad69ad928e8") }, "shard" : "shard0001" }
{ "_id" : "test.lines-_id_ObjectId('4d60b83e874a8ad69ad928e8')", "lastmod" : { "t" : 6000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b83e874a8ad69ad928e8") }, "max" : { "_id" : ObjectId("4d60b840874a8ad69ad95853") }, "shard" : "shard0000" }
{ "_id" : "test.lines-_id_ObjectId('4d60b840874a8ad69ad95853')", "lastmod" : { "t" : 7000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b840874a8ad69ad95853") }, "max" : { "_id" : ObjectId("4d60b842874a8ad69ad985db") }, "shard" : "shard0001" }
{ "_id" : "test.lines-_id_ObjectId('4d60b842874a8ad69ad985db')", "lastmod" : { "t" : 8000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b842874a8ad69ad985db") }, "max" : { "_id" : ObjectId("4d60b843874a8ad69ad9b4c2") }, "shard" : "shard0000" }
{ "_id" : "test.lines-_id_ObjectId('4d60b843874a8ad69ad9b4c2')", "lastmod" : { "t" : 9000, "i" : 0 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b843874a8ad69ad9b4c2") }, "max" : { "_id" : ObjectId("4d60b844874a8ad69ad9e750") }, "shard" : "shard0001" }
{ "_id" : "test.lines-_id_ObjectId('4d60b844874a8ad69ad9e750')", "lastmod" : { "t" : 9000, "i" : 1 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b844874a8ad69ad9e750") }, "max" : { "_id" : ObjectId("4d60b845874a8ad69ada1c71") }, "shard" : "shard0002" }
{ "_id" : "test.lines-_id_ObjectId('4d60b845874a8ad69ada1c71')", "lastmod" : { "t" : 3000, "i" : 16 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b845874a8ad69ada1c71") }, "max" : { "_id" : ObjectId("4d60b846874a8ad69ada4720") }, "shard" : "shard0002" }
{ "_id" : "test.lines-_id_ObjectId('4d60b846874a8ad69ada4720')", "lastmod" : { "t" : 3000, "i" : 18 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b846874a8ad69ada4720") }, "max" : { "_id" : ObjectId("4d60b848874a8ad69ada8756") }, "shard" : "shard0002" }
{ "_id" : "test.lines-_id_ObjectId('4d60b848874a8ad69ada8756')", "lastmod" : { "t" : 3000, "i" : 19 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b848874a8ad69ada8756") }, "max" : { "_id" : { $maxKey : 1 } }, "shard" : "shard0002" }
*/
        com.mongodb.BasicDBObject query = new com.mongodb.BasicDBObject();
        query.put("ns", uri.getDatabase()+"."+uri.getCollection());

        com.mongodb.DBCursor cur = chunksCollection.find(query);
        com.mongodb.BasicDBObject sort = new com.mongodb.BasicDBObject();
        cur.sort(sort);
        int num_chunks = 0;

        final List<InputSplit> splits = new ArrayList<InputSplit>( cur.size() );
        while (cur.hasNext()) {
            num_chunks++;
            final com.mongodb.DBObject row = cur.next();
            com.mongodb.DBObject minObj = ((com.mongodb.DBObject) row.get("min"));
            String keyname = minObj.keySet().iterator().next();
            //the shard key can be of any type so this must be an Object
            Object thisMinVal = minObj.get(keyname);
            //System.out.println("this_min_val is a "+this_min_val.getClass().getName());
            Object thisMaxVal = ((com.mongodb.DBObject) row.get("max")).get(keyname);
            Map shardKeyQueryMap = new HashMap();
            if ( ! (thisMinVal instanceof String))
                shardKeyQueryMap.put("$gte",  thisMinVal); //$min does not work
            if (! (thisMaxVal instanceof String))
                shardKeyQueryMap.put("$lt", thisMaxVal);//$max does not work
            com.mongodb.BasicDBObject newQuery = new com.mongodb.BasicDBObject(origQueryMap);
            newQuery.put(keyname,shardKeyQueryMap); //todo: check that original query didn't have a query on the shard key.  If it did merge the queries
            System.out.println(" new_query is: "+newQuery);

            MongoURI inputURI = conf.getInputURI();
            if (useShards){
                final String shardname = (String) row.get("shard");
                String host = shardMap.get(shardname);
                inputURI = getNewURI(inputURI, host);
            }
            splits.add( new MongoInputSplit(  inputURI , newQuery, conf.getFields(), conf.getSort(), conf.getLimit(), conf.getSkip() ) );
        }//while
        System.out.println(" there were "+num_chunks+" chunks returning "+splits.size()+" splits");
        return splits;
    }

    private static com.mongodb.MongoURI getNewURI(com.mongodb.MongoURI orig_uri, String new_server_uri){
        String orig_uri_str = orig_uri.toString();
        orig_uri_str = orig_uri_str.substring(com.mongodb.MongoURI.MONGODB_PREFIX.length());
        
        //uris look like: mongodb://fred:foobar@server1[,server2]/path?options
        
        int server_end = -1;
        int server_start = 0;
        

        int idx = orig_uri_str.lastIndexOf( "/" );
        if ( idx < 0 ){
            server_end = orig_uri_str.length();
        }else {
            server_end = idx;
        }
        idx = orig_uri_str.indexOf( "@" );

        if ( idx > 0 ){
            server_start = idx + 1;
        }
        StringBuilder sb = new StringBuilder(orig_uri_str);
        sb.replace(server_start, server_end, new_server_uri);
        String ans = com.mongodb.MongoURI.MONGODB_PREFIX + sb.toString();
        log.debug("getNewURI(): original "+orig_uri+" new uri: "+ans);
        return new com.mongodb.MongoURI(ans);
    }

    public boolean verifyConfiguration( Configuration conf ){
        return true;
    }
}
