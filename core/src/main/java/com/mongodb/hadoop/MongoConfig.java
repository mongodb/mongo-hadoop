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

// Mongo

import com.mongodb.*;
import com.mongodb.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;

// Hadoop
// Commons

/**
 * Configuration helper tool for MongoDB related Map/Reduce jobs Instance based, more idiomatic for those who prefer it
 * to the static methoding of ConfigUtil
 */
public class MongoConfig {

    /**
     * You probably don't want to use this.
     */
    @Deprecated
    public MongoConfig(){
        _conf = new Configuration();
    }

    public MongoConfig( Configuration conf ){
        _conf = conf;
    }

    public MongoConfig( DataInput in ) throws IOException{
        _conf = new Configuration();
        _conf.readFields( in );
    }

    public boolean isJobVerbose(){
        return MongoConfigUtil.isJobVerbose( _conf );
    }

    public void setJobVerbose( boolean val ){
        MongoConfigUtil.setJobVerbose( _conf, val );
    }

    public boolean isJobBackground(){
        return MongoConfigUtil.isJobBackground( _conf );
    }

    public void setJobBackground( boolean val ){
        MongoConfigUtil.setJobBackground( _conf, val );
    }

    public Class<? extends Mapper> getMapper(){
        return MongoConfigUtil.getMapper( _conf );
    }

    public void setMapper( Class<? extends Mapper> val ){
        MongoConfigUtil.setMapper( _conf, val );
    }

    public Class<?> getMapperOutputKey(){
        return MongoConfigUtil.getMapperOutputKey( _conf );
    }

    public void setMapperOutputKey( Class<?> val ){
        MongoConfigUtil.setMapperOutputKey( _conf, val );
    }

    public Class<?> getMapperOutputValue(){
        return MongoConfigUtil.getMapperOutputValue( _conf );
    }

    public void setMapperOutputValue( Class<?> val ){
        MongoConfigUtil.setMapperOutputKey( _conf, val );
    }

    public Class<? extends Reducer> getCombiner(){
        return MongoConfigUtil.getCombiner( _conf );
    }

    public void setCombiner( Class<? extends Reducer> val ){
        MongoConfigUtil.setCombiner( _conf, val );
    }

    public Class<? extends Reducer> getReducer(){
        return MongoConfigUtil.getReducer( _conf );
    }

    public void setReducer( Class<? extends Reducer> val ){
        MongoConfigUtil.setReducer( _conf, val );
    }

    public Class<? extends Partitioner> getPartitioner(){
        return MongoConfigUtil.getPartitioner( _conf );
    }

    public void setPartitioner( Class<? extends Partitioner> val ){
        MongoConfigUtil.setPartitioner( _conf, val );
    }

    public Class<? extends RawComparator> getSortComparator(){
        return MongoConfigUtil.getSortComparator( _conf );
    }

    public void setSortComparator( Class<? extends RawComparator> val ){
        MongoConfigUtil.setSortComparator( _conf, val );
    }

    public Class<? extends OutputFormat> getOutputFormat(){
        return MongoConfigUtil.getOutputFormat( _conf );
    }

    public void setOutputFormat( Class<? extends OutputFormat> val ){
        MongoConfigUtil.setOutputFormat( _conf, val );
    }

    public Class<?> getOutputKey(){
        return MongoConfigUtil.getOutputKey( _conf );
    }

    public void setOutputKey( Class<?> val ){
        MongoConfigUtil.setOutputKey( _conf, val );
    }

    public Class<?> getOutputValue(){
        return MongoConfigUtil.getOutputValue( _conf );
    }

    public void setOutputValue( Class<?> val ){
        MongoConfigUtil.setOutputValue( _conf, val );
    }

    public Class<? extends InputFormat> getInputFormat(){
        return MongoConfigUtil.getInputFormat( _conf );
    }

    public void setInputFormat( Class<? extends InputFormat> val ){
        MongoConfigUtil.setInputFormat( _conf, val );
    }

    public MongoURI getMongoURI( String key ){
        return MongoConfigUtil.getMongoURI( _conf, key );
    }

    public MongoURI getInputURI(){
        return MongoConfigUtil.getInputURI( _conf );
    }

    public MongoURI getAuthURI(){
        return MongoConfigUtil.getAuthURI( _conf );
    }

    public DBCollection getOutputCollection(){
        return MongoConfigUtil.getOutputCollection( _conf );
    }

    public DBCollection getInputCollection(){
        return MongoConfigUtil.getInputCollection( _conf );
    }

    public void setMongoURI( String key, MongoURI value ){
        MongoConfigUtil.setMongoURI( _conf, key, value );
    }

    public void setMongoURIString( String key, String value ){
        MongoConfigUtil.setMongoURIString( _conf, key, value );
    }

    public void setInputURI( String uri ){
        MongoConfigUtil.setInputURI( _conf, uri );
    }

    public void setInputURI( MongoURI uri ){
        MongoConfigUtil.setInputURI( _conf, uri );
    }

    public void setAuthUri( String uri ) {
        MongoConfigUtil.setAuthURI( _conf, uri );
    }

    public MongoURI getOutputURI(){
        return MongoConfigUtil.getOutputURI( _conf );
    }

    public void setOutputURI( String uri ){
        MongoConfigUtil.setOutputURI( _conf, uri );
    }

    public void setOutputURI( MongoURI uri ){
        MongoConfigUtil.setOutputURI( _conf, uri );
    }

    /**
     * Set JSON but first validate it's parseable into a BSON Object
     */
    public void setJSON( String key, String value ){
        MongoConfigUtil.setJSON( _conf, key, value );
    }

    public DBObject getDBObject( String key ){
        return MongoConfigUtil.getDBObject( _conf, key );
    }

    public void setDBObject( String key, DBObject value ){
        MongoConfigUtil.setDBObject( _conf, key, value );
    }

    public void setQuery( String query ){
        MongoConfigUtil.setQuery( _conf, query );
    }

    public void setQuery( DBObject query ){
        MongoConfigUtil.setQuery( _conf, query );
    }

    public DBObject getQuery(){
        return MongoConfigUtil.getQuery( _conf );
    }

    public void setFields( String fields ){
        MongoConfigUtil.setFields( _conf, fields );
    }

    public void setFields( DBObject fields ){
        MongoConfigUtil.setFields( _conf, fields );
    }

    public DBObject getFields(){
        return MongoConfigUtil.getFields( _conf );
    }

    public void setSort( String sort ){
        MongoConfigUtil.setSort( _conf, sort );
    }

    public void setSort( DBObject sort ){
        MongoConfigUtil.setSort( _conf, sort );
    }

    public DBObject getSort(){
        return MongoConfigUtil.getSort( _conf );
    }

    public int getLimit(){
        return MongoConfigUtil.getLimit( _conf );
    }

    public void setLimit( int limit ){
        MongoConfigUtil.setLimit( _conf, limit );
    }

    public int getSkip(){
        return MongoConfigUtil.getSkip( _conf );
    }

    public void setSkip( int skip ){
        MongoConfigUtil.setSkip( _conf, skip );
    }

    public int getSplitSize(){
        return MongoConfigUtil.getSplitSize( _conf );
    }

    public void setSplitSize( int value ){
        MongoConfigUtil.setSplitSize( _conf, value );
    }

    /**
     * if TRUE,
     * Splits will be read by connecting to the individual shard servers,
     *  however this really isn't safe unless you know what you're doing.
     *  ( issue has to do with chunks moving / relocating during balancing phases)
     * @return
     */
   public boolean canReadSplitsFromShards() {
        return MongoConfigUtil.canReadSplitsFromShards( _conf );
    }

    public void setReadSplitsFromShards( boolean value ){
        MongoConfigUtil.setReadSplitsFromShards( _conf, value );
    }

    /**
     * If sharding is enabled,
     * Use the sharding configured chunks to split up data.
     */
    public boolean isShardChunkedSplittingEnabled() {
        return MongoConfigUtil.isShardChunkedSplittingEnabled( _conf );
    }

    public void setShardChunkSplittingEnabled( boolean value) {
        MongoConfigUtil.setShardChunkSplittingEnabled( _conf, value );
    }
    

    public boolean isRangeQueryEnabled(){
        return MongoConfigUtil.isRangeQueryEnabled( _conf );
    }

    public void setRangeQueryEnabled(boolean value){
        MongoConfigUtil.setRangeQueryEnabled( _conf, value );
    }

    public boolean canReadSplitsFromSecondary() {
        return MongoConfigUtil.canReadSplitsFromSecondary( _conf );
    }

    public void setReadSplitsFromSecondary( boolean value ) {
        MongoConfigUtil.setReadSplitsFromSecondary( _conf, value );
    }
    
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
    public String getInputSplitKeyPattern() {
        return MongoConfigUtil.getInputSplitKeyPattern( _conf );
    }

    // Convenience by DBObject rather than "Pattern"
    public DBObject getInputSplitKey() {
        return MongoConfigUtil.getInputSplitKey( _conf );
    }

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
    public void setInputSplitKeyPattern( String pattern ) {
        MongoConfigUtil.setInputSplitKeyPattern( _conf, pattern );
    }

    // Convenience by DBObject rather than "Pattern"
    public void setInputSplitKey( DBObject key ) {
        MongoConfigUtil.setInputSplitKey( _conf, key );
    }
            
    /**
     * If {@code true}, the driver will attempt to split the MongoDB Input data (if reading from Mongo) into
     * multiple InputSplits to allow parallelism/concurrency in processing within Hadoop.  That is to say,
     * Hadoop will assign one InputSplit per mapper.
     *
     * This is {@code true} by default now, but if {@code false}, only one InputSplit (your whole collection) will be
     * assigned to Hadoop – severely reducing parallel mapping.
     */
    public boolean createInputSplits() {
        return MongoConfigUtil.createInputSplits( _conf );
    }

    /**
     * If {@code true}, the driver will attempt to split the MongoDB Input data (if reading from Mongo) into
     * multiple InputSplits to allow parallelism/concurrency in processing within Hadoop.  That is to say,
     * Hadoop will assign one InputSplit per mapper.
     *
     * This is {@code true} by default now, but if {@code false}, only one InputSplit (your whole collection) will be
     * assigned to Hadoop – severely reducing parallel mapping.
     */
    public void setCreateInputSplits( boolean value ) {
        MongoConfigUtil.setCreateInputSplits( _conf, value );
    }

    /**
     * The MongoDB field to read from for the Mapper Input.
     *
     * This will be fed to your mapper as the "Key" for the input.
     *
     * Defaults to {@code _id}
     */
    public String getInputKey() {
        return MongoConfigUtil.getInputKey( _conf );
    }

    /**
     * The MongoDB field to read from for the Mapper Input.
     *
     * This will be fed to your mapper as the "Key" for the input.
     *
     * Defaults to {@code _id}
     */
    public void setInputKey( String fieldName ) {
        MongoConfigUtil.setInputKey( _conf, fieldName );
    }
    
    public boolean isNoTimeout() {
        return MongoConfigUtil.isNoTimeout( _conf );
    }
    
    public void setNoTimeout( boolean value ) {
        MongoConfigUtil.setNoTimeout( _conf, value );
    }

    final Configuration _conf;

    private static final Log LOG = LogFactory.getLog( MongoConfig.class );
}

