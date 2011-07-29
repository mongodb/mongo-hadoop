package com.mongodb.hadoop.util;

import com.mongodb.*;
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.input.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

import java.util.*;

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
                    "skip() and limit() is not currently supported do to input split issues." );
        }

        /**
         * On the jobclient side we want *ONLY* the min and max ids for each
         * split; Actual querying will be done on the individual mappers.
         */
        //connecting to the individual backend mongods is not safe, do not do so by default
        final boolean useShards = conf.canReadSplitsFromShards();

        final boolean useChunks = conf.isShardChunkedSplittingEnabled();

        final boolean slaveOk = conf.canReadSplitsFromSecondary();

        List<InputSplit> splits = null;

        if ( useShards || useChunks ){
            try {
                MongoURI uri = conf.getInputURI();
                Mongo mongo = uri.connect();
                try {
                    DB db = mongo.getDB( uri.getDatabase() );
                    DBCollection coll = db.getCollection( uri.getCollection() );
                    final CommandResult stats = coll.getStats();
                    final boolean isSharded = stats.getBoolean( "sharded", false );
                    if ( isSharded ){ //don't proceed if the collection isn't actually sharded
                        if ( useChunks )
                            splits = fetchSplitsViaChunks( conf, uri, mongo, useShards, slaveOk );
                        else if ( useShards ){
                            log.warn( "Fetching Input Splits directly from shards is potentially dangerous for data "
                                      + "consistency should migrations occur during the retrieval." );
                            splits = fetchSplitsFromShards( conf, uri, mongo, slaveOk );
                        }
                        if ( splits == null )
                            log.warn( "Failed to create/calculate Input Splits from Shard Chunks." );
                    }
                    else{
                        log.info( "Collection is not sharded, will process as a single Input Split." );
                        log.debug( "Sharding Stats Dump: " + stats );
                    }
                }
                finally {
                    if ( mongo != null )
                        mongo.close();
                    mongo = null;
                }

                if ( splits != null ){

                    if ( log.isDebugEnabled() ){
                        log.debug( "Calculated splits and returning them - splits: " + splits );
                    }

                    return splits;
                }

                log.warn( "Fallthrough code; splits is null." );
                //If splits is null fall through to code below
            }
            catch ( Exception e ) {
                log.error( "Could not get splits (use_shards: " + useShards + ", use_chunks: " + useChunks + ")", e );
            }
        }

        // Number of *documents*, not bytes, to split on
        final int splitSize = conf.getSplitSize();
        splits = new ArrayList<InputSplit>( 1 );
        // no splits, no sharding
        splits.add( new MongoInputSplit( conf.getInputURI(), conf.getQuery(), conf.getFields(), conf.getSort(),
                                         conf.getLimit(), conf.getSkip() ) );


        if ( log.isDebugEnabled() ){
            log.debug( "Calculated " + splits.size() + " split objects." );
        }

        return splits;
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
            splits.add( new MongoInputSplit( thisUri, conf.getQuery(), conf.getFields(), conf.getSort(),
                                             conf.getLimit(), conf.getSkip() ) );
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

            final List<InputSplit> splits = new ArrayList<InputSplit>( numExpectedChunks );
            while ( cur.hasNext() ){
                numChunks++;
                final BasicDBObject row = (BasicDBObject) cur.next();
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
                    final String shardname = row.getString( "shard" );

                    String host = shardMap.get( shardname );

                    inputURI = getNewURI( inputURI, host, slaveOk );
                }
                splits.add(
                        new MongoInputSplit( inputURI, shardKeyQuery, conf.getFields(), conf.getSort(), conf.getLimit(),
                                             conf.getSkip() ) );
            }

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
