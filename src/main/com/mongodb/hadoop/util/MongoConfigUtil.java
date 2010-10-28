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

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.*;
import com.mongodb.util.*;

/**
 * Configuration helper tool for MongoDB related Map/Reduce jobs
 **/

public class MongoConfigUtil {
    private static final Log log = LogFactory.getLog( MongoConfigUtil.class );

    private static final Mongo.Holder _mongos = new Mongo.Holder();

    /**
     * The JOB_* values are entirely optional and disregarded
     * unless you use the MongoTool base toolset... If you don't,
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

    public static final String INPUT_QUERY = "mongo.input.query";
    public static final String INPUT_FIELDS = "mongo.input.fields";
    public static final String INPUT_SORT = "mongo.input.sort";
    public static final String INPUT_LIMIT = "mongo.input.limit";
    public static final String INPUT_SKIP = "mongo.input.skip";
    // Number of *documents*, not bytes, to split on
    public static final String INPUT_SPLIT_SIZE = "mongo.input.split_size";

    // Number of *documents*, not bytes, to split on
    public static final int DEFAULT_SPLIT_SIZE = 1024; // 1000 docs per split

    public static boolean isJobVerbose( Configuration conf ){
        return conf.getBoolean( JOB_VERBOSE, false );
    }

    public static void setJobVerbose( Configuration conf , boolean val ){
        conf.setBoolean( JOB_VERBOSE, val );
    }

    public static boolean isJobBackground( Configuration conf ){
        return conf.getBoolean( JOB_BACKGROUND, false );
    }

    public static void setJobBackground( Configuration conf , boolean val ){
        conf.setBoolean( JOB_BACKGROUND, val );
    }

    // TODO - In light of key/value specifics should we have a base MongoMapper
    // class?
    public static Class<? extends Mapper> getMapper( Configuration conf ){
        /** TODO - Support multiple inputs via getClasses ? **/
        return conf.getClass( JOB_MAPPER, null, Mapper.class );
    }

    public static void setMapper( Configuration conf , Class<? extends Mapper> val ){
        conf.setClass( JOB_MAPPER, val, Mapper.class );
    }

    public static Class<?> getMapperOutputKey( Configuration conf ){
        return conf.getClass( JOB_MAPPER_OUTPUT_KEY, null );
    }

    public static void setMapperOutputKey( Configuration conf , Class<?> val ){
        conf.setClass( JOB_MAPPER_OUTPUT_KEY, val, Object.class );
    }

    public static Class<?> getMapperOutputValue( Configuration conf ){
        return conf.getClass( JOB_MAPPER_OUTPUT_VALUE, null );
    }

    public static void setMapperOutputValue( Configuration conf , Class<?> val ){
        conf.setClass( JOB_MAPPER_OUTPUT_VALUE, val, Object.class );
    }

    public static Class<? extends Reducer> getCombiner( Configuration conf ){
        return conf.getClass( JOB_COMBINER, null, Reducer.class );
    }

    public static void setCombiner( Configuration conf , Class<? extends Reducer> val ){
        conf.setClass( JOB_COMBINER, val, Reducer.class );
    }

    // TODO - In light of key/value specifics should we have a base MongoReducer
    // class?
    public static Class<? extends Reducer> getReducer( Configuration conf ){
        /** TODO - Support multiple outputs via getClasses ? **/
        return conf.getClass( JOB_REDUCER, null, Reducer.class );
    }

    public static void setReducer( Configuration conf , Class<? extends Reducer> val ){
        conf.setClass( JOB_REDUCER, val, Reducer.class );
    }

    public static Class<? extends Partitioner> getPartitioner( Configuration conf ){
        return conf.getClass( JOB_PARTITIONER, null, Partitioner.class );
    }

    public static void setPartitioner( Configuration conf , Class<? extends Partitioner> val ){
        conf.setClass( JOB_PARTITIONER, val, Partitioner.class );
    }

    public static Class<? extends RawComparator> getSortComparator( Configuration conf ){
        return conf.getClass( JOB_SORT_COMPARATOR, null, RawComparator.class );
    }

    public static void setSortComparator( Configuration conf , Class<? extends RawComparator> val ){
        conf.setClass( JOB_SORT_COMPARATOR, val, RawComparator.class );
    }

    public static Class<? extends OutputFormat> getOutputFormat( Configuration conf ){
        return conf.getClass( JOB_OUTPUT_FORMAT, null, OutputFormat.class );
    }

    public static void setOutputFormat( Configuration conf , Class<? extends OutputFormat> val ){
        conf.setClass( JOB_OUTPUT_FORMAT, val, OutputFormat.class );
    }

    public static Class<?> getOutputKey( Configuration conf ){
        return conf.getClass( JOB_OUTPUT_KEY, null );
    }

    public static void setOutputKey( Configuration conf , Class<?> val ){
        conf.setClass( JOB_OUTPUT_KEY, val, Object.class );
    }

    public static Class<?> getOutputValue( Configuration conf ){
        return conf.getClass( JOB_OUTPUT_VALUE, null );
    }

    public static void setOutputValue( Configuration conf , Class<?> val ){
        conf.setClass( JOB_OUTPUT_VALUE, val, Object.class );
    }

    public static Class<? extends InputFormat> getInputFormat( Configuration conf ){
        return conf.getClass( JOB_INPUT_FORMAT, null, InputFormat.class );
    }

    public static void setInputFormat( Configuration conf , Class<? extends InputFormat> val ){
        conf.setClass( JOB_INPUT_FORMAT, val, InputFormat.class );
    }

    public static MongoURI getMongoURI( Configuration conf , String key ){
        final String raw = conf.get( key );
        if ( raw != null && !raw.trim().isEmpty() )
            return new MongoURI( raw );
        else
            return null;
    }

    public static MongoURI getInputURI( Configuration conf ){
        return getMongoURI( conf, INPUT_URI );
    }

    public static DBCollection getCollection( MongoURI uri ){
        try {
            return uri.connectCollection( _mongos.connect( uri ) );
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Unable to connect to collection." , e );
        }
    }

    public static DBCollection getOutputCollection( Configuration conf ){
        try {
            final MongoURI _uri = getOutputURI( conf );
            return getCollection( _uri );
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Unable to connect to MongoDB Output Collection." , e );
        }
    }

    public static DBCollection getInputCollection( Configuration conf ){
        try {
            final MongoURI _uri = getInputURI( conf );
            return getCollection( _uri );
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Unable to connect to MongoDB Input Collection at '" + getInputURI( conf ) + "'" , e );
        }
    }

    public static void setMongoURI( Configuration conf , String key , MongoURI value ){
        conf.set( key, value.toString() ); // todo - verify you can toString a
                                           // URI object
    }

    public static void setMongoURIString( Configuration conf , String key , String value ){

        try {
            final MongoURI uri = new MongoURI( value );
            setMongoURI( conf, key, uri );
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Invalid Mongo URI '" + value + "' for Input URI" , e );
        }
    }

    public static void setInputURI( Configuration conf , String uri ){
        setMongoURIString( conf, INPUT_URI, uri );
    }

    public static void setInputURI( Configuration conf , MongoURI uri ){
        setMongoURI( conf, INPUT_URI, uri );
    }

    public static MongoURI getOutputURI( Configuration conf ){
        return getMongoURI( conf, OUTPUT_URI );
    }

    public static void setOutputURI( Configuration conf , String uri ){
        setMongoURIString( conf, OUTPUT_URI, uri );
    }

    public static void setOutputURI( Configuration conf , MongoURI uri ){
        setMongoURI( conf, OUTPUT_URI, uri );
    }

    /**
     * Set JSON but first validate it's parseable into a DBObject
     */
    public static void setJSON( Configuration conf , String key , String value ){
        try {
            final Object dbObj = JSON.parse( value );
            setDBObject( conf, key, (DBObject) dbObj );
        }
        catch ( final Exception e ) {
            log.error( "Cannot parse JSON...", e );
            throw new IllegalArgumentException( "Provided JSON String is not representable/parseable as a DBObject." , e );
        }
    }

    public static DBObject getDBObject( Configuration conf , String key ){
        try {
            final String json = conf.get( key );
            final DBObject obj = (DBObject) JSON.parse( json );
            if ( obj == null )
                return new BasicDBObject();
            else
                return obj;
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Provided JSON String is not representable/parseable as a DBObject." , e );
        }
    }

    public static void setDBObject( Configuration conf , String key , DBObject value ){
        conf.set( key, JSON.serialize( value ) );
    }

    public static void setQuery( Configuration conf , String query ){
        setJSON( conf, INPUT_QUERY, query );
    }

    public static void setQuery( Configuration conf , DBObject query ){
        setDBObject( conf, INPUT_QUERY, query );
    }

    /**
     * Returns the configured query as a DBObject...
     * If you want a string call toString() on the returned object.
     * or use JSON.serialize()
     **/
    public static DBObject getQuery( Configuration conf ){
        return getDBObject( conf, INPUT_QUERY );
    }

    public static void setFields( Configuration conf , String fields ){
        setJSON( conf, INPUT_FIELDS, fields );
    }

    public static void setFields( Configuration conf , DBObject fields ){
        setDBObject( conf, INPUT_FIELDS, fields );
    }

    /**
     * Returns the configured fields as a DBObject...
     * If you want a string call toString() on the returned object.
     * or use JSON.serialize()
     **/
    public static DBObject getFields( Configuration conf ){
        return getDBObject( conf, INPUT_FIELDS );
    }

    public static void setSort( Configuration conf , String sort ){
        setJSON( conf, INPUT_SORT, sort );
    }

    public static void setSort( Configuration conf , DBObject sort ){
        setDBObject( conf, INPUT_SORT, sort );
    }

    /**
     * Returns the configured sort as a DBObject...
     * If you want a string call toString() on the returned object.
     * or use JSON.serialize()
     **/
    public static DBObject getSort( Configuration conf ){
        return getDBObject( conf, INPUT_SORT );
    }

    public static int getLimit( Configuration conf ){
        return conf.getInt( INPUT_LIMIT, 0 );
    }

    public static void setLimit( Configuration conf , int limit ){
        conf.setInt( INPUT_LIMIT, limit );
    }

    public static int getSkip( Configuration conf ){
        return conf.getInt( INPUT_SKIP, 0 );
    }

    public static void setSkip( Configuration conf , int skip ){
        conf.setInt( INPUT_SKIP, skip );
    }

    public static int getSplitSize( Configuration conf ){
        return conf.getInt( INPUT_SPLIT_SIZE, DEFAULT_SPLIT_SIZE );
    }

    public static void setSplitSize( Configuration conf , int value ){
        conf.setInt( INPUT_SPLIT_SIZE, value );
    }

}
