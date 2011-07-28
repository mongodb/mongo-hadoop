/*
 * Copyright 2011 10gen Inc.
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

// Mongo

import com.mongodb.*;
import com.mongodb.hadoop.input.*;
import com.mongodb.hadoop.util.*;
import org.bson.BSONObject;

// Commons
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;

// Java
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

public class MongoInputFormat extends InputFormat<Object, BSONObject> {

    @Override
    public RecordReader<Object, BSONObject> createRecordReader( InputSplit split, TaskAttemptContext context ){
        if ( !( split instanceof MongoInputSplit ) )
            throw new IllegalStateException( "Creation of a new RecordReader requires a MongoInputSplit instance." );

        final MongoInputSplit mis = (MongoInputSplit) split;

        return new com.mongodb.hadoop.input.MongoRecordReader( mis );
    }

    @Override
    public List<InputSplit> getSplits( JobContext context ){
        final Configuration hadoopConfiguration = context.getConfiguration();
        final MongoConfig conf = new MongoConfig( hadoopConfiguration );
        return getSplits( hadoopConfiguration, conf );
    }

    // public because called by the other MongoInputFormat
    public List<InputSplit> getSplits( Configuration hadoopConfiguration, MongoConfig conf ){

        if ( conf.getLimit() > 0 || conf.getSkip() > 0 ){
            /**
             * TODO - If they specify skip or limit we create only one input
             * split
             */
            throw new IllegalArgumentException( "skip() and limit() is not currently supported do to input split issues." );
        }

        /**
         * On the jobclient side we want *ONLY* the min and max ids for each
         * split; Actual querying will be done on the individual mappers.
         */
        //connecting to the individual backend mongods is not safe, do not do so by default
        final boolean useShards
                = hadoopConfiguration.getBoolean( MongoConfigUtil.SPLITS_USE_SHARDS, false );

        final boolean useChunks
                = hadoopConfiguration.getBoolean( MongoConfigUtil.SPLITS_USE_CHUNKS, true );

        Boolean slaveOk = null;
        String slaveOkProp = hadoopConfiguration.get( MongoConfigUtil.SPLITS_SLAVE_OK );

        if ( slaveOkProp != null )
            slaveOk = Boolean.valueOf( slaveOkProp );

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
                            splits = getSplitsUsingChunks( conf, uri, mongo, useShards, slaveOk );
                        else if ( useShards )
                            splits = getSplitsUsingShards( conf, uri, mongo, slaveOk );
                    }
                }
                finally {
                    if ( mongo != null )
                        mongo.close();

                    mongo = null;
                }

                if ( splits != null ) {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Calculated splits and returning them - splits: " + splits);
                    }

                    return splits;
                }

                LOG.warn( "Fallthrough code; splits is null." );
                //If splits is null fall through to code below
            }
            catch ( Exception e ) {
                LOG.error( "Could not get splits (use_shards: " + useShards + ", use_chunks: " + useChunks + ")", e );
            }
        }

        // Number of *documents*, not bytes, to split on
        final int splitSize = conf.getSplitSize();
        splits = new ArrayList<InputSplit>( 1 );
        // no splits, no sharding
        splits.add( new MongoInputSplit( conf.getInputURI(), conf.getQuery(), conf.getFields(), conf.getSort(),
                                         conf.getLimit(), conf.getSkip() ) );


        if (LOG.isDebugEnabled()) {
            LOG.debug( "Calculated " + splits.size() + " split objects." );
        }

        return splits;
    }

    /**
     * This gets the URIs to the backend {@code mongod}s and returns splits that connect directly to those backends (one
     * split for each backend). There are two potential problems with this: <ol><li>clients that can connect to {@code
     * mongos} can't necessarily connect to the individual {@code mongod}s. <li>there concurrency issues (if chunks are
     * in the process of getting moved around). </ol>
     */
    private List<InputSplit> getSplitsUsingShards( final MongoConfig conf,
                                                   MongoURI uri, Mongo mongo, Boolean slaveOk ){
        LOG.warn( "WARNING getting splits that connect directly to the backend mongods"
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
     * This constructs splits using the chunk boundries.
     */
    private List<InputSplit> getSplitsUsingChunks( final MongoConfig conf,
                                                   MongoURI uri,
                                                   Mongo mongo,
                                                   boolean useShards,
                                                   Boolean slaveOk )
    {
        DBObject originalQuery = conf.getQuery();

        if ( useShards )
            LOG.warn( "WARNING getting splits that connect directly to the backend mongods"
                      + " is risky and might not produce correct results" );

        if (LOG.isDebugEnabled()) {
            LOG.debug( "getSplitsUsingChunks(): originalQuery: " + originalQuery );
        }

        DB configDB = mongo.getDB( "config" );

        Map<String, String> shardMap = null; //key: shardname, value: host

        if ( useShards ) {

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
            } finally {
                if ( cur != null )
                    cur.close();
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug( "MongoInputFormat.getSplitsUsingChunks(): shard map is: " + shardMap );
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

            final List<InputSplit> splits = new ArrayList<InputSplit>( cur.size() );
            while ( cur.hasNext() ){
                numChunks++;
                final BasicDBObject row = (BasicDBObject) cur.next();
                DBObject minObj = ( (DBObject) row.get( "min" ) );
                String keyname = minObj.keySet().iterator().next();
                //the shard key can be of any type so this must be an Object
                Object thisMinVal = minObj.get( keyname );
                Object thisMaxVal = ( (DBObject) row.get( "max" ) ).get( keyname );
                DBObject shardKeyQuery = new BasicDBObject();
                if ( !( thisMinVal == SplitFriendlyDBCallback.MIN_KEY_TYPE || thisMinVal.equals( "MinKey") ) )
                    shardKeyQuery.put( "$min", new BasicDBObject().append( keyname, thisMinVal ) );
                if ( !( thisMaxVal == SplitFriendlyDBCallback.MAX_KEY_TYPE || thisMaxVal.equals( "MaxKey") ) )
                    shardKeyQuery.put( "$max", new BasicDBObject().append( keyname, thisMaxVal ) );
                //must put something for $query or will silently fail. If no original query use an empty DBObject
                if ( originalQuery == null )
                    originalQuery = new BasicDBObject();
                shardKeyQuery.put( "$query", originalQuery );

                if (LOG.isDebugEnabled()) {
                    LOG.debug( "[" + numChunks + "/" + splits.size() + "] new query is: " + shardKeyQuery );
                }

                MongoURI inputURI = conf.getInputURI();

                if ( useShards ){
                    final String shardname = row.getString( "shard" );

                    String host = shardMap.get( shardname );

                    inputURI = getNewURI( inputURI, host, slaveOk );
                }
                splits.add( new MongoInputSplit( inputURI, shardKeyQuery, conf.getFields(), conf.getSort(), conf.getLimit(),
                                                 conf.getSkip() ) );
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug( "MongoInputFormat.getSplitsUsingChunks(): There were "
                          + numChunks
                          + " chunks, returning "
                          + splits.size()
                          + " splits: " + splits );
            }

            return splits;

        } finally {
            if ( cur != null )
                cur.close();
        }
    }

    private static MongoURI getNewURI( MongoURI originalUri, String newServerUri, Boolean slaveok ) {

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
        LOG.debug( "getNewURI(): original " + originalUri + " new uri: " + ans );
        return new MongoURI( ans );
    }

    public boolean verifyConfiguration( Configuration conf ){
        return true;
    }

    private static final Log LOG = LogFactory.getLog( MongoInputFormat.class );
}
