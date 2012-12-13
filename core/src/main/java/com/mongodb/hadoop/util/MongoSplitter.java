package com.mongodb.hadoop.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.input.MongoInputSplit;

/**
 * Copyright (c) 2010, 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

public class MongoSplitter {

    public static List<InputSplit> calculateSplits( MongoConfig conf ){

        if ( conf.getLimit() > 0 || conf.getSkip() > 0 ){
            /**
             * TODO - If they specify skip or limit we create only one input
             * split
             */
            throw new IllegalArgumentException(
                    "skip() and limit() is not currently supported due to input split issues." );
        }

        /**
         * On the jobclient side we want *ONLY* the min and max ids for each
         * split; Actual querying will be done on the individual mappers.
         */
        MongoURI uri = conf.getInputURI();
        DBCollection coll = MongoConfigUtil.getCollection(uri);
        DB db = coll.getDB(); 
        Mongo mongo = db.getMongo();
        
        final CommandResult stats = coll.getStats();
        
        final boolean isSharded = stats.getBoolean( "sharded", false );

        //connecting to the individual backend mongods is not safe, do not do so by default
        final boolean useShards = conf.canReadSplitsFromShards();

        final boolean useChunks = conf.isShardChunkedSplittingEnabled();

        final boolean slaveOk = conf.canReadSplitsFromSecondary();

        log.info(" Calculate Splits Code ... Use Shards? " + useShards + ", Use Chunks? " + useChunks + "; Collection Sharded? " + isSharded);
        if (conf.createInputSplits()) {
            log.info( "Creation of Input Splits is enabled." );
            if (isSharded && (useShards || useChunks)){  // todo I don't think these settings can be run together
                if (useShards && useChunks)
                    log.warn( "Combining 'use chunks' and 'read from shards directly' can have unexpected & erratic behavior in a live system due to chunk migrations. " );

                log.info( "Sharding mode calculation entering." );
                return calculateShardedSplits( conf, useShards, useChunks, slaveOk, uri, mongo );
            }
            else { // perfectly ok for sharded setups to run with a normally calculated split. May even be more efficient for some cases
                log.info( "Using Unsharded Split mode (Calculating multiple splits though)" );
                return calculateUnshardedSplits( conf, slaveOk, uri, coll );
            }
            
        }
        else {
            log.info( "Creation of Input Splits is disabled; Non-Split mode calculation entering." );
            return calculateSingleSplit( conf );
        }
    }

    private static List<InputSplit> calculateUnshardedSplits( MongoConfig conf, boolean slaveOk, 
                                                              MongoURI uri, DBCollection coll ){
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        final DBObject splitKey = conf.getInputSplitKey(); // a bit slower but forces validation of the JSON
        final int splitSize = conf.getSplitSize(); // in MB
        final String ns = coll.getFullName();
        final DBObject q = conf.getQuery();
        
        log.info( "Calculating unsharded input splits on namespace '" + ns + "' with Split Key '" + splitKey.toString() + "' and a split size of '" + splitSize + "'mb per" );

        final DBObject cmd = BasicDBObjectBuilder.start("splitVector", ns).
                                          add( "keyPattern", splitKey ).
                                          add( "force", false ). // force:True is misbehaving it seems
                                          add( "maxChunkSize", splitSize ).get();
        
        log.trace( "Issuing Command: " + cmd );
        CommandResult data = coll.getDB().command( cmd );

        if ( data.containsField( "$err" ) )
            throw new IllegalArgumentException( "Error calculating splits: " + data );
        else if ( (Double) data.get( "ok" ) != 1.0 )
            throw new IllegalArgumentException( "Unable to calculate input splits: " + ( (String) data.get( "errmsg" ) ) );
        
        // Comes in a format where "min" and "max" are implicit and each entry is just a boundary key; not ranged
        BasicDBList splitData = (BasicDBList) data.get( "splitKeys" );
        
        if (splitData.size() <= 1) {
            if (splitData.size() < 1)
                log.warn( "WARNING: No Input Splits were calculated by the split code. "
                          + "Proceeding with a *single* split. Data may be too small, try lowering 'mongo.input.split_size' "
                          + "if this is undesirable." );
            splits.add( _split( conf, q, null, null ) ); // no splits really. Just do the whole thing data is likely small
        }
        else {
            log.info( "Calculated " + splitData.size() + " splits." );

            DBObject lastKey = (DBObject) splitData.get( 0 );

            splits.add( _split( conf, q, null, lastKey ) ); // first "min" split

            for (int i = 1; i < splitData.size(); i++ ) {
                final DBObject _tKey = (DBObject) splitData.get( i );
                splits.add( _split( conf, q, lastKey, _tKey) );
                lastKey = _tKey;
            }

            splits.add( _split( conf, q, lastKey, null ) ); // last "max" split
        }

        return splits;

    }

    private static MongoInputSplit _split( MongoConfig conf, DBObject q, DBObject min, DBObject max ) {
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start( "$query", q );
        if (min != null) // min ceiling
           b.add( "$min", min );
        
        if (max != null) // max ceiling
            b.add( "$max", max );

        final DBObject query = b.get();
        log.trace( "Assembled Query: " + query );

        return new MongoInputSplit( conf.getInputURI(), conf.getInputKey(), query, conf.getFields(), 
                                    conf.getSort(), conf.getLimit(), conf.getSkip(), conf.isNoTimeout() );
    }
    
    private static List<InputSplit> calculateSingleSplit( MongoConfig conf ){
        final List<InputSplit> splits = new ArrayList<InputSplit>( 1 );
        // no splits, no sharding
        splits.add( new MongoInputSplit( conf.getInputURI(), conf.getInputKey(), conf.getQuery(), 
                                         conf.getFields(), conf.getSort(), conf.getLimit(), conf.getSkip(),
                                         conf.isNoTimeout() ) );


        log.info( "Calculated " + splits.size() + " split objects." );
        log.debug( "Dump of calculated splits ... " );
        for ( InputSplit split : splits ) {
            log.debug("\t Split: " + split.toString());
        }

        return splits;
    }

    private static List<InputSplit> calculateShardedSplits(MongoConfig conf, boolean useShards, boolean useChunks, boolean slaveOk, MongoURI uri, Mongo mongo) {
        final List<InputSplit> splits;
        try {
            if ( useChunks )
                splits = fetchSplitsViaChunks( conf, uri, mongo, useShards, slaveOk );
            else if ( useShards ){
                log.warn( "Fetching Input Splits directly from shards is potentially dangerous for data "
                          + "consistency should migrations occur during the retrieval." );
                splits = fetchSplitsFromShards( conf, uri, mongo, slaveOk );
            }
            else throw new IllegalStateException( "Neither useChunks nor useShards enabled; failed to pick a valid state. " );

            if ( splits == null )
                throw new IllegalStateException( "Failed to create/calculate Input Splits from Shard Chunks; final splits content is 'null'." );

            if ( log.isDebugEnabled() ){
                log.debug( "Calculated splits and returning them - splits: " + splits );
            }

            return splits;
        }
        catch ( Exception e ) {
            log.error( "Could not get splits (use_shards: " + useShards + ", use_chunks: " + useChunks + ")", e );
            throw new IllegalStateException(e);
        }
    }

    /**
     * This gets the URIs to the backend {@code mongod}s and returns splits that connect directly to those backends (one
     * split for each backend). There are two potential problems with this: <ol><li>clients that can connect to {@code
     * mongos} can't necessarily connect to the individual {@code mongod}s. <li>there concurrency issues (if chunks are
     * in the process of getting moved around). </ol>
     */
    private static List<InputSplit> fetchSplitsFromShards( final MongoConfig conf,
                                                           MongoURI uri,
                                                           Mongo mongo,
                                                           Boolean slaveOk ){
        log.warn( "WARNING getting splits that connect directly to the backend mongods"
                  + " is risky and might not produce correct results" );
        DB configDb = mongo.getDB( "config" );
        DBCollection shardsColl = configDb.getCollection( "shards" );

        Set<String> shardSet = new java.util.HashSet<String>();

        DBCursor cur = shardsColl.find();
        try {
            while ( cur.hasNext() ){
                final BasicDBObject row = (BasicDBObject) cur.next();
                String host = row.getString( "host" );
                int slashIndex = host.indexOf( '/' );
                if ( slashIndex > 0 )
                    host = host.substring( slashIndex + 1 );
                shardSet.add( host );
            }
        }
        finally {
            if ( cur != null )
                cur.close();
            cur = null;
        }
        final List<InputSplit> splits = new ArrayList<InputSplit>( shardSet.size() );
        //todo: using stats only get the shards that actually host data for this collection
        for ( String host : shardSet ){
            MongoURI thisUri = getNewURI( uri, host, slaveOk );
            splits.add( new MongoInputSplit( thisUri, conf.getInputKey(), conf.getQuery(), conf.getFields(),
                                             conf.getSort(), conf.getLimit(), conf.getSkip(), 
                                             conf.isNoTimeout() ) ); // TODO - Should the input Key be the shard key?
        }
        return splits;
    }

    /**
     * This constructs splits using the chunk boundaries.
     */
    private static List<InputSplit> fetchSplitsViaChunks( final MongoConfig conf,
                                                          MongoURI uri,
                                                          Mongo mongo,
                                                          boolean useShards,
                                                          Boolean slaveOk ){
        DBObject originalQuery = conf.getQuery();

        if ( useShards )
            log.warn( "WARNING getting splits that connect directly to the backend mongods"
                      + " is risky and might not produce correct results" );

        if ( log.isDebugEnabled() ){
            log.debug( "getSplitsUsingChunks(): originalQuery: " + originalQuery );
        }

        DB configDB = mongo.getDB( "config" );

        Map<String, String> shardMap = null; //key: shardname, value: host

        if ( useShards ){

            shardMap = new HashMap<String, String>();
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
            }
            finally {
                if ( cur != null )
                    cur.close();
            }
        }

        if ( log.isDebugEnabled() ){
            log.debug( "MongoInputFormat.getSplitsUsingChunks(): shard map is: " + shardMap );
        }

        DBCollection chunksCollection = configDB.getCollection( "chunks" );

        /* Chunks looks like:
        { "_id" : "test.lines-_id_ObjectId('4d60b839874a8ad69ad8adf6')", "lastmod" : { "t" : 3000, "i" : 1 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b839874a8ad69ad8adf6") }, "max" : { "_id" : ObjectId("4d60b83a874a8ad69ad8d1a9") }, "shard" : "shard0000" }
        { "_id" : "test.lines-_id_ObjectId('4d60b848874a8ad69ada8756')", "lastmod" : { "t" : 3000, "i" : 19 }, "ns" : "test.lines", "min" : { "_id" : ObjectId("4d60b848874a8ad69ada8756") }, "max" : { "_id" : { $maxKey : 1 } }, "shard" : "shard0002" }
        */

        BasicDBObject query = new BasicDBObject();
        query.put( "ns", uri.getDatabase() + "." + uri.getCollection() );

        DBCursor cur = chunksCollection.find( query );

        try {
            int numChunks = 0;
            final int numExpectedChunks = cur.size();
            Map<String, LinkedList<InputSplit>> shardToSplits = new HashMap<String, LinkedList<InputSplit>>();

            while ( cur.hasNext() ){
                numChunks++;
                final BasicDBObject row = (BasicDBObject) cur.next();
                final String shardName = row.getString( "shard" );

                DBObject minObj = ( (DBObject) row.get( "min" ) );
                DBObject shardKeyQuery = new BasicDBObject();
                DBObject min = new BasicDBObject();
                DBObject max = new BasicDBObject();

                for ( String keyName : minObj.keySet() ){
                    Object tMin = minObj.get( keyName );
                    Object tMax = ( (DBObject) row.get( "max" ) ).get( keyName );
                    /** The shard key can be of any possible type, so this must be kept as Object */
                    if ( !( tMin == SplitFriendlyDBCallback.MIN_KEY_TYPE || tMin.equals( "MinKey" ) ) )
                        min.put( keyName, tMin );
                    if ( !( tMax == SplitFriendlyDBCallback.MAX_KEY_TYPE || tMax.equals( "MaxKey" ) ) )
                        max.put( keyName, tMax );
                }
                /** We have to put something for $query or we'll fail; if no original query use an empty DBObj */
                if ( originalQuery == null )
                    originalQuery = new BasicDBObject();
                shardKeyQuery.put( "$min", min );
                shardKeyQuery.put( "$max", max );
                shardKeyQuery.put( "$query", originalQuery );

                if ( log.isDebugEnabled() ){
                    log.debug( "[" + numChunks + "/" + numExpectedChunks + "] new query is: " + shardKeyQuery );
                }

                MongoURI inputURI = conf.getInputURI();

                if ( useShards ){
                    String host = shardMap.get( shardName );
                    inputURI = getNewURI( inputURI, host, slaveOk );
                }
                
                LinkedList<InputSplit> shardSplits = shardToSplits.get(shardName);
                if (shardSplits == null) {
                    shardSplits = new LinkedList<InputSplit>();
                    shardToSplits.put(shardName, shardSplits);
                }
                
                MongoInputSplit mongoInputSplit = new MongoInputSplit( inputURI, conf.getInputKey(), shardKeyQuery, conf.getFields(), conf.getSort(),  // TODO - should inputKey be the shard key?
                                     conf.getLimit(), conf.getSkip(), 
                                     conf.isNoTimeout() );
                shardSplits.add( mongoInputSplit );
            }
            
            final List<InputSplit> splits = createSplitList(numChunks,
                    shardToSplits);

            if ( log.isDebugEnabled() ){
                log.debug( "MongoInputFormat.getSplitsUsingChunks(): There were "
                           + numChunks
                           + " chunks, returning "
                           + splits.size()
                           + " splits: " + splits );
            }

            return splits;

        }
        finally {
            if ( cur != null )
                cur.close();
        }
    }

    /**
     * Round robin splits across shards.  The splits are going to end up as Map jobs
     * processed in the same order as the splits.  We want to have continuous map
     * jobs be on separate shards so that as you're completing map jobs the work
     * is spread evenly across shard machines.
     * 
     * @param numChunks - Number of chunks
     * @param shardToSplits - Map of shardName to list of splits on that shard.
     * @return
     */
    protected static List<InputSplit> createSplitList(int numChunks,
            Map<String, LinkedList<InputSplit>> shardToSplits) {
        final List<InputSplit> splits = new ArrayList<InputSplit>( numChunks );
        int splitIndex = 0;
        while (splitIndex < numChunks) {
            Set<String> shardSplitsToRemove = new HashSet<String>();
            for (Map.Entry<String, LinkedList<InputSplit>> shardSplits: shardToSplits.entrySet()) {
                LinkedList<InputSplit> shardSplitsList = shardSplits.getValue();
                InputSplit split = shardSplitsList.pop();
                splits.add(splitIndex, split);
                splitIndex++;
                if (shardSplitsList.isEmpty()) {
                    shardSplitsToRemove.add(shardSplits.getKey());
                }
            }
            for (String shardName : shardSplitsToRemove) {
                shardToSplits.remove(shardName);
            }
        }
        return splits;
    }

    private static MongoURI getNewURI( MongoURI originalUri, String newServerUri, Boolean slaveok ){

        String originalUriString = originalUri.toString();
        originalUriString = originalUriString.substring( MongoURI.MONGODB_PREFIX.length() );

        // uris look like: mongodb://fred:foobar@server1[,server2]/path?options

        int serverEnd = -1;
        int serverStart = 0;


        int idx = originalUriString.lastIndexOf( "/" );
        if ( idx < 0 ){
            serverEnd = originalUriString.length();
        }
        else{
            serverEnd = idx;
        }
        idx = originalUriString.indexOf( "@" );

        if ( idx > 0 ){
            serverStart = idx + 1;
        }
        StringBuilder sb = new StringBuilder( originalUriString );
        sb.replace( serverStart, serverEnd, newServerUri );
        if ( slaveok != null ){
            //If uri already contains options append option to end of uri.
            //This will override any slaveok option already in the uri
            if ( originalUriString.contains( "?" ) )
                sb.append( "&slaveok=" ).append( slaveok );
            else
                sb.append( "?slaveok=" ).append( slaveok );
        }
        String ans = MongoURI.MONGODB_PREFIX + sb.toString();
        log.debug( "getNewURI(): original " + originalUri + " new uri: " + ans );
        return new MongoURI( ans );
    }

    private static final Log log = LogFactory.getLog( MongoSplitter.class );
}
