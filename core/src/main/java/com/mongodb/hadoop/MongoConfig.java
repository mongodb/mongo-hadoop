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
import com.mongodb.MongoClientURI;
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
        MongoConfigUtil.setMapperOutputValue(configuration, val);
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

    public MongoClientURI getMongoURI(final String key) {
        return MongoConfigUtil.getMongoClientURI(configuration, key);
    }

    public MongoClientURI getInputURI() {
        return MongoConfigUtil.getInputURI(configuration);
    }

    public MongoClientURI getAuthURI() {
        return MongoConfigUtil.getAuthURI(configuration);
    }

    public DBCollection getOutputCollection() {
        return MongoConfigUtil.getOutputCollection(configuration);
    }

    public DBCollection getInputCollection() {
        return MongoConfigUtil.getInputCollection(configuration);
    }

    public void setMongoURI(final String key, final MongoURI value) {
        setMongoURI(key, new MongoClientURI(value.toString()));
    }

    public void setMongoURI(final String key, final MongoClientURI value) {
        MongoConfigUtil.setMongoURI(configuration, key, value);
    }

    public void setMongoURIString(final String key, final String value) {
        MongoConfigUtil.setMongoURIString(configuration, key, value);
    }

    public void setInputURI(final String uri) {
        MongoConfigUtil.setInputURI(configuration, uri);
    }

    /**
     * @deprecated use {@link #setInputURI(MongoClientURI)} instead
     * @param uri the input URI
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public void setInputURI(final MongoURI uri) {
        MongoConfigUtil.setInputURI(configuration, uri);
    }

    /**
     * Set the input URI for the job.
     * @param uri the input URI
     */
    public void setInputURI(final MongoClientURI uri) {
        MongoConfigUtil.setInputURI(configuration, uri);
    }

    public void setAuthUri(final String uri) {
        MongoConfigUtil.setAuthURI(configuration, uri);
    }

    public MongoClientURI getOutputURI() {
        return MongoConfigUtil.getOutputURI(configuration);
    }

    public void setOutputURI(final String uri) {
        MongoConfigUtil.setOutputURI(configuration, uri);
    }

    /**
     * @deprecated use {@link #setOutputURI(MongoClientURI)} instead.
     * @param uri the output URI.
     */
    @Deprecated
    @SuppressWarnings("deprecation")    
    public void setOutputURI(final MongoURI uri) {
        MongoConfigUtil.setOutputURI(configuration, uri);
    }

    /**
     * Set the output URI for the job.
     * @param uri the output URI
     */
    public void setOutputURI(final MongoClientURI uri) {
        MongoConfigUtil.setOutputURI(configuration, uri);
    }

    /**
     * Helper to set a JSON string for a given key.
     * @param key the key
     * @param value the JSON string
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

    public boolean canReadSplitsFromShards() {
        return MongoConfigUtil.canReadSplitsFromShards(configuration);
    }

    /**
     * Set whether the reading directly from shards is enabled.
     *
     * When {@code true}, splits are read directly from shards. By default,
     * splits are read through a mongos router when connected to a sharded
     * cluster. Note that reading directly from shards can lead to bizarre
     * results when there are orphaned documents or if the balancer is running.
     *
     * @param value enables reading from shards directly
     *
     * @see <a href="http://docs.mongodb.org/manual/core/sharding-balancing/">Sharding Balancing</a>
     * @see <a href="http://docs.mongodb.org/manual/reference/command/cleanupOrphaned/#dbcmd.cleanupOrphaned">cleanupOrphaned command</a>
     */
    public void setReadSplitsFromShards(final boolean value) {
        MongoConfigUtil.setReadSplitsFromShards(configuration, value);
    }

    public boolean isShardChunkedSplittingEnabled() {
        return MongoConfigUtil.isShardChunkedSplittingEnabled(configuration);
    }

    /**
     * Set whether using shard chunk splits as InputSplits is enabled.
     * @param value enables using shard chunk splits as InputSplits.
     */
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

    public String getInputSplitKeyPattern() {
        return MongoConfigUtil.getInputSplitKeyPattern(configuration);
    }

    // Convenience by DBObject rather than "Pattern"
    public DBObject getInputSplitKey() {
        return MongoConfigUtil.getInputSplitKey(configuration);
    }

    /**
     * If {@code CREATE_INPUT_SPLITS} is true but {@code SPLITS_USE_CHUNKS} is
     * false, the connector will attempt to create InputSplits using the
     * MongoDB {@code splitVector} command. By default it will split on
     * {@code {"_id": 1}}.
     *
     * If you want to customize that split point for efficiency reasons
     * (such as different distribution) you may set this to any valid field
     * name. The rules for this field are the same as for MongoDB shard keys.
     * You must have an index on the field, and follow the other rules outlined
     * in the docs.
     *
     * This must be a JSON document, and not just a field name!
     * @param pattern the JSON document describing the index to use for
     *                splitVector.
     * @see <a href="http://docs.mongodb.org/manual/core/sharding-shard-key/">Shard Keys</a>
     * @see <a href="http://docs.mongodb.org/manual/reference/command/splitVector/">splitVector</a>
     */
    public void setInputSplitKeyPattern(final String pattern) {
        MongoConfigUtil.setInputSplitKeyPattern(configuration, pattern);
    }

    // Convenience by DBObject rather than "Pattern"
    public void setInputSplitKey(final DBObject key) {
        MongoConfigUtil.setInputSplitKey(configuration, key);
    }

    public boolean createInputSplits() {
        return MongoConfigUtil.createInputSplits(configuration);
    }

    /**
     * Set whether creating InputSplits is enabled. If {@code true}, the driver
     * will attempt to split the MongoDB input data (if reading from Mongo)
     * into multiple InputSplits to allow parallelism when processing within
     * Hadoop. If {@code false}, only one InputSplit will be created for the
     * entire input. This option is {@code true} by default.
     *
     * @param value enables creating multiple InputSplits.
     */
    public void setCreateInputSplits(final boolean value) {
        MongoConfigUtil.setCreateInputSplits(configuration, value);
    }

    public String getInputKey() {
        return MongoConfigUtil.getInputKey(configuration);
    }

    /**
     * Set the field for each document to use as the Mapper's key.
     * This is {@code _id} by default.
     * @param fieldName the field to use as the Mapper key.
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

