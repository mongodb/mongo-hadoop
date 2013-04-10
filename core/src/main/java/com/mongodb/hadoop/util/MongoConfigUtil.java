// MongoConfigUtil.java
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

package com.mongodb.hadoop.util;

import com.mongodb.*;
import com.mongodb.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.util.*;

/**
 * Configuration helper tool for MongoDB related Map/Reduce jobs
 */

public class MongoConfigUtil {
    private static final Log log = LogFactory.getLog( MongoConfigUtil.class );

    private static final Mongo.Holder _mongos = new Mongo.Holder();

    /**
     * The JOB_* values are entirely optional and disregarded unless you use the MongoTool base toolset... If you don't,
     * feel free to ignore these
     */
    public static final String JOB_VERBOSE = "mongo.job.verbose";
    public static final String JOB_BACKGROUND = "mongo.job.background";

    public static final String JOB_MAPPER = "mongo.job.mapper";
    public static final String JOB_COMBINER = "mongo.job.combiner";
    public static final String JOB_PARTITIONER = "mongo.job.partitioner";
    public static final String JOB_REDUCER = "mongo.job.reducer";
    public static final String JOB_SORT_COMPARATOR = "mongo.job.sort_comparator";

    public static final String JOB_MAPPER_OUTPUT_KEY = "mongo.job.mapper.output.key";
    public static final String JOB_MAPPER_OUTPUT_VALUE = "mongo.job.mapper.output.value";

    public static final String JOB_INPUT_FORMAT = "mongo.job.input.format";
    public static final String JOB_OUTPUT_FORMAT = "mongo.job.output.format";

    public static final String JOB_OUTPUT_KEY = "mongo.job.output.key";
    public static final String JOB_OUTPUT_VALUE = "mongo.job.output.value";

    public static final String INPUT_URI = "mongo.input.uri";
    public static final String OUTPUT_URI = "mongo.output.uri";


    /**
     * The MongoDB field to read from for the Mapper Input.
     *
     * This will be fed to your mapper as the "Key" for the input.
     *
     * Defaults to {@code _id}
     */
    public static final String INPUT_KEY = "mongo.input.key";
    public static final String INPUT_NOTIMEOUT = "mongo.input.notimeout";
    public static final String INPUT_QUERY = "mongo.input.query";
    public static final String INPUT_FIELDS = "mongo.input.fields";
    public static final String INPUT_SORT = "mongo.input.sort";
    public static final String INPUT_LIMIT = "mongo.input.limit";
    public static final String INPUT_SKIP = "mongo.input.skip";

    /**
     * A username and password to use.
     *
     * This is necessary when running jobs with a sharded cluster, as 
     * access to the config database is needed to get 
     *
     */
    public static final String AUTH_URI = "mongo.auth.uri";


    /**
     * When *not* using 'read_from_shards' or 'read_shard_chunks'
     * The number of megabytes per Split to create for the input data.
     *
     * Currently defaults to 8MB, tweak it as necessary for your code.
     *
     * This default will likely change as we research better options.
     */
    public static final String INPUT_SPLIT_SIZE = "mongo.input.split_size";

    public static final int DEFAULT_SPLIT_SIZE = 8; // 8 mb per manual (non-sharding) split

    /**
     * If CREATE_INPUT_SPLITS is true but SPLITS_USE_CHUNKS is false, Mongo-Hadoop will attempt
     * to create custom input splits for you.  By default it will split on {@code _id}, which is a
     * reasonable/sane default.
     *
     * If you want to customize that split point for efficiency reasons (such as different distribution)
     * you may set this to any valid field name. The restriction on this key name are the *exact same rules*
     * as when sharding an existing MongoDB Collection.  You must have an index on the field, and follow the other
     * rules outlined in the docs.
     *
     * This must be a JSON document, and not just a field name!
     *
     * @link http://www.mongodb.org/display/DOCS/Sharding+Introduction#ShardingIntroduction-ShardKeys
     */
    public static final String INPUT_SPLIT_KEY_PATTERN = "mongo.input.split.split_key_pattern";

    /**
     * If {@code true}, the driver will attempt to split the MongoDB Input data (if reading from Mongo) into
     * multiple InputSplits to allow parallelism/concurrency in processing within Hadoop.  That is to say,
     * Hadoop will assign one InputSplit per mapper.
     * 
     * This is {@code true} by default now, but if {@code false}, only one InputSplit (your whole collection) will be
     * assigned to Hadoop – severely reducing parallel mapping.
     */
    public static final String CREATE_INPUT_SPLITS = "mongo.input.split.create_input_splits";

    /**
     * If {@code true} in a sharded setup splits will be made to connect to individual backend {@code mongod}s.  This
     * can be unsafe. If {@code mongos} is moving chunks around you might see duplicate data, or miss some data
     * entirely. Defaults to {@code false}
     */
    public static final String SPLITS_USE_SHARDS = "mongo.input.split.read_from_shards";
    /**
     * If {@code true} have one split = one shard chunk.  If {@link #SPLITS_USE_SHARDS} is not true splits will still
     * use chunks, but will connect through {@code mongos} instead of the individual backend {@code mongod}s (the safe
     * thing to do). If {@link #SPLITS_USE_SHARDS} is {@code true} but this is {@code false} one split will be made for
     * each backend shard. THIS IS UNSAFE and may result in data being run multiple times <p> Defaults to {@code true }
     */
    public static final String SPLITS_USE_CHUNKS = "mongo.input.split.read_shard_chunks";
    /**
     * If true then shards are replica sets run queries on slaves. If set this will override any option passed on the
     * URI.
     *
     * Defaults to {@code false}
     */
    public static final String SPLITS_SLAVE_OK = "mongo.input.split.allow_read_from_secondaries";

    /**
     * If true then queries for splits will be constructed using $lt/$gt instead of $min and $max.
     *
     * Defaults to {@code false}
     */
    public static final String SPLITS_USE_RANGEQUERY = "mongo.input.split.use_range_queries";

    public static boolean isJobVerbose( Configuration conf ){
        return conf.getBoolean( JOB_VERBOSE, false );
    }

    public static void setJobVerbose( Configuration conf, boolean val ){
        conf.setBoolean( JOB_VERBOSE, val );
    }

    public static boolean isJobBackground( Configuration conf ){
        return conf.getBoolean( JOB_BACKGROUND, false );
    }

    public static void setJobBackground( Configuration conf, boolean val ){
        conf.setBoolean( JOB_BACKGROUND, val );
    }

    // TODO - In light of key/value specifics should we have a base MongoMapper
    // class?
    public static Class<? extends Mapper> getMapper( Configuration conf ){
        /** TODO - Support multiple inputs via getClasses ? **/
        return conf.getClass( JOB_MAPPER, null, Mapper.class );
    }

    public static void setMapper( Configuration conf, Class<? extends Mapper> val ){
        conf.setClass( JOB_MAPPER, val, Mapper.class );
    }

    public static Class<?> getMapperOutputKey( Configuration conf ){
        return conf.getClass( JOB_MAPPER_OUTPUT_KEY, null );
    }

    public static void setMapperOutputKey( Configuration conf, Class<?> val ){
        conf.setClass( JOB_MAPPER_OUTPUT_KEY, val, Object.class );
    }

    public static Class<?> getMapperOutputValue( Configuration conf ){
        return conf.getClass( JOB_MAPPER_OUTPUT_VALUE, null );
    }

    public static void setMapperOutputValue( Configuration conf, Class<?> val ){
        conf.setClass( JOB_MAPPER_OUTPUT_VALUE, val, Object.class );
    }

    public static Class<? extends Reducer> getCombiner( Configuration conf ){
        return conf.getClass( JOB_COMBINER, null, Reducer.class );
    }

    public static void setCombiner( Configuration conf, Class<? extends Reducer> val ){
        conf.setClass( JOB_COMBINER, val, Reducer.class );
    }

    // TODO - In light of key/value specifics should we have a base MongoReducer
    // class?
    public static Class<? extends Reducer> getReducer( Configuration conf ){
        /** TODO - Support multiple outputs via getClasses ? **/
        return conf.getClass( JOB_REDUCER, null, Reducer.class );
    }

    public static void setReducer( Configuration conf, Class<? extends Reducer> val ){
        conf.setClass( JOB_REDUCER, val, Reducer.class );
    }

    public static Class<? extends Partitioner> getPartitioner( Configuration conf ){
        return conf.getClass( JOB_PARTITIONER, null, Partitioner.class );
    }

    public static void setPartitioner( Configuration conf, Class<? extends Partitioner> val ){
        conf.setClass( JOB_PARTITIONER, val, Partitioner.class );
    }

    public static Class<? extends RawComparator> getSortComparator( Configuration conf ){
        return conf.getClass( JOB_SORT_COMPARATOR, null, RawComparator.class );
    }

    public static void setSortComparator( Configuration conf, Class<? extends RawComparator> val ){
        conf.setClass( JOB_SORT_COMPARATOR, val, RawComparator.class );
    }

    public static Class<? extends OutputFormat> getOutputFormat( Configuration conf ){
        return conf.getClass( JOB_OUTPUT_FORMAT, null, OutputFormat.class );
    }

    public static void setOutputFormat( Configuration conf, Class<? extends OutputFormat> val ){
        conf.setClass( JOB_OUTPUT_FORMAT, val, OutputFormat.class );
    }

    public static Class<?> getOutputKey( Configuration conf ){
        return conf.getClass( JOB_OUTPUT_KEY, null );
    }

    public static void setOutputKey( Configuration conf, Class<?> val ){
        conf.setClass( JOB_OUTPUT_KEY, val, Object.class );
    }

    public static Class<?> getOutputValue( Configuration conf ){
        return conf.getClass( JOB_OUTPUT_VALUE, null );
    }

    public static void setOutputValue( Configuration conf, Class<?> val ){
        conf.setClass( JOB_OUTPUT_VALUE, val, Object.class );
    }

    public static Class<? extends InputFormat> getInputFormat( Configuration conf ){
        return conf.getClass( JOB_INPUT_FORMAT, null, InputFormat.class );
    }

    public static void setInputFormat( Configuration conf, Class<? extends InputFormat> val ){
        conf.setClass( JOB_INPUT_FORMAT, val, InputFormat.class );
    }

    public static List<MongoURI> getMongoURIs( Configuration conf, String key ){
        final String raw = conf.get( key );
        if (raw != null && !raw.trim().isEmpty() ) {
            List<MongoURI> result = new LinkedList<MongoURI>();
            String[] split = StringUtils.split(raw);
            for (String mongoURI : split) {
                result.add(new MongoURI(mongoURI));
            }
            return result;
        }
        else
            return Collections.emptyList();
    }

    public static MongoURI getMongoURI( Configuration conf, String key ){
        final String raw = conf.get( key );
        if ( raw != null && !raw.trim().isEmpty() )
            return new MongoURI( raw );
        else
            return null;
    }

    public static MongoURI getInputURI( Configuration conf ){
        return getMongoURI( conf, INPUT_URI );
    }

    public static MongoURI getAuthURI( Configuration conf ){
        return getMongoURI( conf, AUTH_URI );
    }

    public static List<DBCollection> getCollections( List<MongoURI> uris ){
        List<DBCollection> dbCollections = new LinkedList<DBCollection>();
        for (MongoURI uri : uris) {
            dbCollections.add(getCollection(uri));
        }
        return dbCollections;
    }

    public static DBCollection getCollection( MongoURI uri ){
        try {
            Mongo mongo = _mongos.connect( uri );
            DB myDb = mongo.getDB(uri.getDatabase());

            //if there's a username and password
            if(uri.getUsername() != null && uri.getPassword() != null && !myDb.isAuthenticated()) {
                boolean auth = myDb.authenticate(uri.getUsername(), uri.getPassword());
                if(auth) {
                    log.info("Sucessfully authenticated with collection.");
                }
                else {
                    throw new IllegalArgumentException( "Unable to connect to collection." );
                }
            }
            return uri.connectCollection(mongo);
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Unable to connect to collection." + e.getMessage(), e );
        }
    }

    public static DBCollection getOutputCollection( Configuration conf ){
        try {
            final MongoURI _uri = getOutputURI(conf);
            return getCollection(_uri);
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Unable to connect to MongoDB Output Collection.", e );
        }
    }

    public static List<DBCollection> getOutputCollections( Configuration conf ){
        try {
            final List<MongoURI> _uris = getOutputURIs(conf);
            return getCollections(_uris);
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Unable to connect to MongoDB Output Collection.", e );
        }
    }

    public static DBCollection getInputCollection( Configuration conf ){
        try {
            final MongoURI _uri = getInputURI(conf);
            return getCollection( _uri );
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException(
                    "Unable to connect to MongoDB Input Collection at '" + getInputURI( conf ) + "'", e );
        }
    }

    public static void setMongoURI( Configuration conf, String key, MongoURI value ){
        conf.set( key, value.toString() ); // todo - verify you can toString a
        // URI object
    }

    public static void setMongoURIString( Configuration conf, String key, String value ){

        try {
            final MongoURI uri = new MongoURI( value );
            setMongoURI( conf, key, uri );
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Invalid Mongo URI '" + value + "' for Input URI", e );
        }
    }

    public static void setAuthURI( Configuration conf, String uri ){
        setMongoURIString( conf, AUTH_URI, uri );
    }

    public static void setInputURI( Configuration conf, String uri ){
        setMongoURIString( conf, INPUT_URI, uri );
    }

    public static void setInputURI( Configuration conf, MongoURI uri ){
        setMongoURI(conf, INPUT_URI, uri);
    }

    public static List<MongoURI> getOutputURIs( Configuration conf ){
        return getMongoURIs(conf, OUTPUT_URI);
    }

    public static MongoURI getOutputURI( Configuration conf ){
        return getMongoURI( conf, OUTPUT_URI );
    }

    public static void setOutputURI( Configuration conf, String uri ){
        setMongoURIString( conf, OUTPUT_URI, uri );
    }

    public static void setOutputURI( Configuration conf, MongoURI uri ){
        setMongoURI( conf, OUTPUT_URI, uri );
    }

    /**
     * Set JSON but first validate it's parseable into a DBObject
     */
    public static void setJSON( Configuration conf, String key, String value ){
        try {
            final Object dbObj = JSON.parse( value );
            setDBObject( conf, key, (DBObject) dbObj );
        }
        catch ( final Exception e ) {
            log.error( "Cannot parse JSON...", e );
            throw new IllegalArgumentException( "Provided JSON String is not representable/parseable as a DBObject.",
                                                e );
        }
    }

    public static DBObject getDBObject( Configuration conf, String key ){
        try {
            final String json = conf.get( key );
            final DBObject obj = (DBObject) JSON.parse( json );
            if ( obj == null )
                return new BasicDBObject();
            else
                return obj;
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Provided JSON String is not representable/parseable as a DBObject.",
                                                e );
        }
    }

    public static void setDBObject( Configuration conf, String key, DBObject value ){
        conf.set( key, JSON.serialize( value ) );
    }

    public static void setQuery( Configuration conf, String query ){
        setJSON( conf, INPUT_QUERY, query );
    }

    public static void setQuery( Configuration conf, DBObject query ){
        setDBObject( conf, INPUT_QUERY, query );
    }

    /**
     * Returns the configured query as a DBObject... If you want a string call toString() on the returned object. or use
     * JSON.serialize()
     */
    public static DBObject getQuery( Configuration conf ){
        return getDBObject( conf, INPUT_QUERY );
    }

    public static void setFields( Configuration conf, String fields ){
        setJSON( conf, INPUT_FIELDS, fields );
    }

    public static void setFields( Configuration conf, DBObject fields ){
        setDBObject( conf, INPUT_FIELDS, fields );
    }

    /**
     * Returns the configured fields as a DBObject... If you want a string call toString() on the returned object. or
     * use JSON.serialize()
     */
    public static DBObject getFields( Configuration conf ){
        return getDBObject( conf, INPUT_FIELDS );
    }

    public static void setSort( Configuration conf, String sort ){
        setJSON( conf, INPUT_SORT, sort );
    }

    public static void setSort( Configuration conf, DBObject sort ){
        setDBObject( conf, INPUT_SORT, sort );
    }

    /**
     * Returns the configured sort as a DBObject... If you want a string call toString() on the returned object. or use
     * JSON.serialize()
     */
    public static DBObject getSort( Configuration conf ){
        return getDBObject( conf, INPUT_SORT );
    }

    public static int getLimit( Configuration conf ){
        return conf.getInt( INPUT_LIMIT, 0 );
    }

    public static void setLimit( Configuration conf, int limit ){
        conf.setInt( INPUT_LIMIT, limit );
    }

    public static int getSkip( Configuration conf ){
        return conf.getInt( INPUT_SKIP, 0 );
    }

    public static void setSkip( Configuration conf, int skip ){
        conf.setInt( INPUT_SKIP, skip );
    }

    public static int getSplitSize( Configuration conf ){
        return conf.getInt( INPUT_SPLIT_SIZE, DEFAULT_SPLIT_SIZE );
    }

    public static void setSplitSize( Configuration conf, int value ){
        conf.setInt( INPUT_SPLIT_SIZE, value );
    }

    /**
     * if TRUE,
     * Splits will be queried using $lt/$gt instead of $max and $min.
     * This allows the database's query optimizer to choose the best index, 
     * instead of being forced to use the one in the $max/$min keys.
     * This will only work if the key used for splitting is *not* a compound key.
     * Make sure that all values under the splitting key are of the same type, or
     * this will cause incomplete results.
     * @return
     */
    public static boolean isRangeQueryEnabled( Configuration conf ){
        return conf.getBoolean( SPLITS_USE_RANGEQUERY, false );
    }

    public static void setRangeQueryEnabled( Configuration conf, boolean value ){
        conf.setBoolean( SPLITS_USE_RANGEQUERY, value );
    }

    /**
     * if TRUE,
     * Splits will be read by connecting to the individual shard servers,
     * Only use this 
     *  ( issue has to do with chunks moving / relocating during balancing phases)
     * @return
     */
    public static boolean canReadSplitsFromShards( Configuration conf ){
        return conf.getBoolean( SPLITS_USE_SHARDS, false );
    }

    public static void setReadSplitsFromShards( Configuration conf, boolean value ){
        conf.setBoolean( SPLITS_USE_SHARDS, value );
    }

    /**
     * If sharding is enabled,
     * Use the sharding configured chunks to split up data.
     */
    public static boolean isShardChunkedSplittingEnabled( Configuration conf ) {
        return conf.getBoolean( SPLITS_USE_CHUNKS, true );
    }

    public static void setShardChunkSplittingEnabled( Configuration conf, boolean value) {
        conf.setBoolean( SPLITS_USE_CHUNKS, value );
    }

    public static boolean canReadSplitsFromSecondary( Configuration conf ) {
        return conf.getBoolean( SPLITS_SLAVE_OK, false );
    }

    public static void setReadSplitsFromSecondary( Configuration conf, boolean value ) {
        conf.getBoolean( SPLITS_SLAVE_OK, value );
    }

    public static boolean createInputSplits( Configuration conf ) {
        return conf.getBoolean( CREATE_INPUT_SPLITS, true );
    }

    public static void setCreateInputSplits( Configuration conf, boolean value ) {
        conf.setBoolean( CREATE_INPUT_SPLITS, value );
    }

    public static void setInputSplitKeyPattern( Configuration conf, String pattern ) {
        setJSON( conf, INPUT_SPLIT_KEY_PATTERN, pattern );
    }

    public static void setInputSplitKey( Configuration conf, DBObject key ) {
        setDBObject( conf, INPUT_SPLIT_KEY_PATTERN, key );
    }
    
    public static String getInputSplitKeyPattern( Configuration conf ) {
        return conf.get( INPUT_SPLIT_KEY_PATTERN, "{ \"_id\": 1 }" );
    }
    
    public static DBObject getInputSplitKey( Configuration conf ) {
        try {
            final String json = getInputSplitKeyPattern( conf );
            final DBObject obj = (DBObject) JSON.parse( json );
            if ( obj == null )
                return new BasicDBObject("_id", 1);
            else
                return obj;
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Provided JSON String is not representable/parseable as a DBObject.", e );
        }
    }


    public static void setInputKey( Configuration conf, String fieldName ) {
        // TODO (bwm) - validate key rules?
        conf.set( INPUT_KEY, fieldName );
    }
    
    public static String getInputKey( Configuration conf ) {
        return conf.get( INPUT_KEY, "_id" );
    }
   
    public static void setNoTimeout( Configuration conf, boolean value ) {
        conf.setBoolean( INPUT_NOTIMEOUT, value );
    }
    
    public static boolean isNoTimeout( Configuration conf ) {
        return conf.getBoolean( INPUT_NOTIMEOUT, false );
    }
}
