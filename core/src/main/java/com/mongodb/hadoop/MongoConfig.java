/*
 * Copyright 2010-2013 10gen Inc.
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

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.IOException;

/**
 * Configuration helper tool for MongoDB related Map/Reduce jobs Instance based, more idiomatic for those who prefer it to the static
 * methods of ConfigUtil
 */
@SuppressWarnings("UnusedDeclaration")
public class MongoConfig {

    private static final Log LOG = LogFactory.getLog(MongoConfig.class);

    private final Configuration configuration;

    /**
     * You probably don't want to use this.
     */
    @Deprecated
    public MongoConfig() {
        configuration = new Configuration();
    }

    public MongoConfig(final Configuration conf) {
        configuration = conf;
    }

    public MongoConfig(final DataInput in) throws IOException {
        configuration = new Configuration();
        configuration.readFields(in);
    }

    public boolean isJobVerbose() {
        return MongoConfigUtil.isJobVerbose(configuration);
    }

    public void setJobVerbose(final boolean val) {
        MongoConfigUtil.setJobVerbose(configuration, val);
    }

    public boolean isJobBackground() {
        return MongoConfigUtil.isJobBackground(configuration);
    }

    public void setJobBackground(final boolean val) {
        MongoConfigUtil.setJobBackground(configuration, val);
    }

    public Class<? extends Mapper> getMapper() {
        return MongoConfigUtil.getMapper(configuration);
    }

    public void setMapper(final Class<? extends Mapper> val) {
        MongoConfigUtil.setMapper(configuration, val);
    }

    public Class<?> getMapperOutputKey() {
        return MongoConfigUtil.getMapperOutputKey(configuration);
    }

    public void setMapperOutputKey(final Class<?> val) {
        MongoConfigUtil.setMapperOutputKey(configuration, val);
    }

    public Class<?> getMapperOutputValue() {
        return MongoConfigUtil.getMapperOutputValue(configuration);
    }

    public void setMapperOutputValue(final Class<?> val) {
        MongoConfigUtil.setMapperOutputKey(configuration, val);
    }

    public Class<? extends Reducer> getCombiner() {
        return MongoConfigUtil.getCombiner(configuration);
    }

    public void setCombiner(final Class<? extends Reducer> val) {
        MongoConfigUtil.setCombiner(configuration, val);
    }

    public Class<? extends Reducer> getReducer() {
        return MongoConfigUtil.getReducer(configuration);
    }

    public void setReducer(final Class<? extends Reducer> val) {
        MongoConfigUtil.setReducer(configuration, val);
    }

    public Class<? extends Partitioner> getPartitioner() {
        return MongoConfigUtil.getPartitioner(configuration);
    }

    public void setPartitioner(final Class<? extends Partitioner> val) {
        MongoConfigUtil.setPartitioner(configuration, val);
    }

    public Class<? extends RawComparator> getSortComparator() {
        return MongoConfigUtil.getSortComparator(configuration);
    }

    public void setSortComparator(final Class<? extends RawComparator> val) {
        MongoConfigUtil.setSortComparator(configuration, val);
    }

    public Class<? extends OutputFormat> getOutputFormat() {
        return MongoConfigUtil.getOutputFormat(configuration);
    }

    public void setOutputFormat(final Class<? extends OutputFormat> val) {
        MongoConfigUtil.setOutputFormat(configuration, val);
    }

    public Class<?> getOutputKey() {
        return MongoConfigUtil.getOutputKey(configuration);
    }

    public void setOutputKey(final Class<?> val) {
        MongoConfigUtil.setOutputKey(configuration, val);
    }

    public Class<?> getOutputValue() {
        return MongoConfigUtil.getOutputValue(configuration);
    }

    public void setOutputValue(final Class<?> val) {
        MongoConfigUtil.setOutputValue(configuration, val);
    }

    public Class<? extends InputFormat> getInputFormat() {
        return MongoConfigUtil.getInputFormat(configuration);
    }

    public void setInputFormat(final Class<? extends InputFormat> val) {
        MongoConfigUtil.setInputFormat(configuration, val);
    }

    public MongoURI getMongoURI(final String key) {
        return MongoConfigUtil.getMongoURI(configuration, key);
    }

    public MongoURI getInputURI() {
        return MongoConfigUtil.getInputURI(configuration);
    }

    public MongoURI getAuthURI() {
        return MongoConfigUtil.getAuthURI(configuration);
    }

    public DBCollection getOutputCollection() {
        return MongoConfigUtil.getOutputCollection(configuration);
    }

    public DBCollection getInputCollection() {
        return MongoConfigUtil.getInputCollection(configuration);
    }

    public void setMongoURI(final String key, final MongoURI value) {
        MongoConfigUtil.setMongoURI(configuration, key, value);
    }

    public void setMongoURIString(final String key, final String value) {
        MongoConfigUtil.setMongoURIString(configuration, key, value);
    }

    public void setInputURI(final String uri) {
        MongoConfigUtil.setInputURI(configuration, uri);
    }

    public void setInputURI(final MongoURI uri) {
        MongoConfigUtil.setInputURI(configuration, uri);
    }

    public void setAuthUri(final String uri) {
        MongoConfigUtil.setAuthURI(configuration, uri);
    }

    public MongoURI getOutputURI() {
        return MongoConfigUtil.getOutputURI(configuration);
    }

    public void setOutputURI(final String uri) {
        MongoConfigUtil.setOutputURI(configuration, uri);
    }

    public void setOutputURI(final MongoURI uri) {
        MongoConfigUtil.setOutputURI(configuration, uri);
    }

    /**
     * Set JSON but first validate it's parseable into a BSON Object
     */
    public void setJSON(final String key, final String value) {
        MongoConfigUtil.setJSON(configuration, key, value);
    }

    public DBObject getDBObject(final String key) {
        return MongoConfigUtil.getDBObject(configuration, key);
    }

    public void setDBObject(final String key, final DBObject value) {
        MongoConfigUtil.setDBObject(configuration, key, value);
    }

    public void setQuery(final String query) {
        MongoConfigUtil.setQuery(configuration, query);
    }

    public void setQuery(final DBObject query) {
        MongoConfigUtil.setQuery(configuration, query);
    }

    public DBObject getQuery() {
        return MongoConfigUtil.getQuery(configuration);
    }

    public void setFields(final String fields) {
        MongoConfigUtil.setFields(configuration, fields);
    }

    public void setFields(final DBObject fields) {
        MongoConfigUtil.setFields(configuration, fields);
    }

    public DBObject getFields() {
        return MongoConfigUtil.getFields(configuration);
    }

    public void setSort(final String sort) {
        MongoConfigUtil.setSort(configuration, sort);
    }

    public void setSort(final DBObject sort) {
        MongoConfigUtil.setSort(configuration, sort);
    }

    public DBObject getSort() {
        return MongoConfigUtil.getSort(configuration);
    }

    public int getLimit() {
        return MongoConfigUtil.getLimit(configuration);
    }

    public void setLimit(final int limit) {
        MongoConfigUtil.setLimit(configuration, limit);
    }

    public int getSkip() {
        return MongoConfigUtil.getSkip(configuration);
    }

    public void setSkip(final int skip) {
        MongoConfigUtil.setSkip(configuration, skip);
    }

    public boolean getLazyBSON() {
        return MongoConfigUtil.getLazyBSON(configuration);
    }

    public void setLazyBSON(final boolean lazy) {
        MongoConfigUtil.setLazyBSON(configuration, lazy);
    }

    public int getSplitSize() {
        return MongoConfigUtil.getSplitSize(configuration);
    }

    public void setSplitSize(final int value) {
        MongoConfigUtil.setSplitSize(configuration, value);
    }

    /**
     * if TRUE, Splits will be read by connecting to the individual shard servers, however this really isn't safe unless you know what
     * you're doing. ( issue has to do with chunks moving / relocating during balancing phases)
     */
    public boolean canReadSplitsFromShards() {
        return MongoConfigUtil.canReadSplitsFromShards(configuration);
    }

    public void setReadSplitsFromShards(final boolean value) {
        MongoConfigUtil.setReadSplitsFromShards(configuration, value);
    }

    /**
     * If sharding is enabled, Use the sharding configured chunks to split up data.
     */
    public boolean isShardChunkedSplittingEnabled() {
        return MongoConfigUtil.isShardChunkedSplittingEnabled(configuration);
    }

    public void setShardChunkSplittingEnabled(final boolean value) {
        MongoConfigUtil.setShardChunkSplittingEnabled(configuration, value);
    }


    public boolean isRangeQueryEnabled() {
        return MongoConfigUtil.isRangeQueryEnabled(configuration);
    }

    public void setRangeQueryEnabled(final boolean value) {
        MongoConfigUtil.setRangeQueryEnabled(configuration, value);
    }

    public boolean canReadSplitsFromSecondary() {
        return MongoConfigUtil.canReadSplitsFromSecondary(configuration);
    }

    public void setReadSplitsFromSecondary(final boolean value) {
        MongoConfigUtil.setReadSplitsFromSecondary(configuration, value);
    }

    /**
     * If CREATE_INPUT_SPLITS is true but SPLITS_USE_CHUNKS is false, Mongo-Hadoop will attempt to create custom input splits for you.  By
     * default it will split on {@code _id}, which is a reasonable/sane default.
     * <p/>
     * If you want to customize that split point for efficiency reasons (such as different distribution) you may set this to any valid field
     * name. The restriction on this key name are the *exact same rules* as when sharding an existing MongoDB Collection.  You must have an
     * index on the field, and follow the other rules outlined in the docs.
     * <p/>
     * This must be a JSON document, and not just a field name!
     *
     * @link http://www.mongodb.org/display/DOCS/Sharding+Introduction#ShardingIntroduction-ShardKeys
     */
    public String getInputSplitKeyPattern() {
        return MongoConfigUtil.getInputSplitKeyPattern(configuration);
    }

    // Convenience by DBObject rather than "Pattern"
    public DBObject getInputSplitKey() {
        return MongoConfigUtil.getInputSplitKey(configuration);
    }

    /**
     * If CREATE_INPUT_SPLITS is true but SPLITS_USE_CHUNKS is false, Mongo-Hadoop will attempt to create custom input splits for you.  By
     * default it will split on {@code _id}, which is a reasonable/sane default.
     * <p/>
     * If you want to customize that split point for efficiency reasons (such as different distribution) you may set this to any valid field
     * name. The restriction on this key name are the *exact same rules* as when sharding an existing MongoDB Collection.  You must have an
     * index on the field, and follow the other rules outlined in the docs.
     * <p/>
     * This must be a JSON document, and not just a field name!
     *
     * @link http://www.mongodb.org/display/DOCS/Sharding+Introduction#ShardingIntroduction-ShardKeys
     */
    public void setInputSplitKeyPattern(final String pattern) {
        MongoConfigUtil.setInputSplitKeyPattern(configuration, pattern);
    }

    // Convenience by DBObject rather than "Pattern"
    public void setInputSplitKey(final DBObject key) {
        MongoConfigUtil.setInputSplitKey(configuration, key);
    }

    /**
     * If {@code true}, the driver will attempt to split the MongoDB Input data (if reading from Mongo) into multiple InputSplits to allow
     * parallelism/concurrency in processing within Hadoop.  That is to say, Hadoop will assign one InputSplit per mapper.
     * <p/>
     * This is {@code true} by default now, but if {@code false}, only one InputSplit (your whole collection) will be assigned to Hadoop –
     * severely reducing parallel mapping.
     */
    public boolean createInputSplits() {
        return MongoConfigUtil.createInputSplits(configuration);
    }

    /**
     * If {@code true}, the driver will attempt to split the MongoDB Input data (if reading from Mongo) into multiple InputSplits to allow
     * parallelism/concurrency in processing within Hadoop.  That is to say, Hadoop will assign one InputSplit per mapper.
     * <p/>
     * This is {@code true} by default now, but if {@code false}, only one InputSplit (your whole collection) will be assigned to Hadoop –
     * severely reducing parallel mapping.
     */
    public void setCreateInputSplits(final boolean value) {
        MongoConfigUtil.setCreateInputSplits(configuration, value);
    }

    /**
     * The MongoDB field to read from for the Mapper Input.
     * <p/>
     * This will be fed to your mapper as the "Key" for the input.
     * <p/>
     * Defaults to {@code _id}
     */
    public String getInputKey() {
        return MongoConfigUtil.getInputKey(configuration);
    }

    /**
     * The MongoDB field to read from for the Mapper Input.
     * <p/>
     * This will be fed to your mapper as the "Key" for the input.
     * <p/>
     * Defaults to {@code _id}
     */
    public void setInputKey(final String fieldName) {
        MongoConfigUtil.setInputKey(configuration, fieldName);
    }

    public boolean isNoTimeout() {
        return MongoConfigUtil.isNoTimeout(configuration);
    }

    public void setNoTimeout(final boolean value) {
        MongoConfigUtil.setNoTimeout(configuration, value);
    }

    //BSON-related config options
    public boolean getBSONReadSplits() {
        return MongoConfigUtil.getBSONReadSplits(configuration);
    }

    public void setBSONReadSplits(final boolean val) {
        MongoConfigUtil.setBSONReadSplits(configuration, val);
    }

    public boolean getBSONWriteSplits() {
        return MongoConfigUtil.getBSONWriteSplits(configuration);
    }

    public void setBSONWriteSplits(final boolean val) {
        MongoConfigUtil.setBSONWriteSplits(configuration, val);
    }

    public boolean getBSONOutputBuildSplits() {
        return MongoConfigUtil.getBSONOutputBuildSplits(configuration);
    }

    public void setBSONOutputBuildSplits(final boolean val) {
        MongoConfigUtil.setBSONOutputBuildSplits(configuration, val);
    }

    public void setBSONPathFilter(final Class<? extends PathFilter> val) {
        MongoConfigUtil.setBSONPathFilter(configuration, val);
    }

    public Class<?> getBSONPathFilter() {
        return MongoConfigUtil.getBSONPathFilter(configuration);
    }
}

