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


package com.mongodb.hadoop.util;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.splitter.MongoSplitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;

import java.util.List;
import java.util.Map;

/**
 * Configuration helper tool for MongoDB related Map/Reduce jobs
 */

public final class MapredMongoConfigUtil {

    /**
     * The JOB_* values are entirely optional and disregarded unless you use the MongoTool base toolset... If you don't, feel free to ignore
     * these
     */

    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_VERBOSE = MongoConfigUtil.JOB_VERBOSE;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_BACKGROUND = MongoConfigUtil.JOB_BACKGROUND;

    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_MAPPER = MongoConfigUtil.JOB_MAPPER;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_COMBINER = MongoConfigUtil.JOB_COMBINER;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_PARTITIONER = MongoConfigUtil.JOB_PARTITIONER;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_REDUCER = MongoConfigUtil.JOB_REDUCER;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_SORT_COMPARATOR = MongoConfigUtil.JOB_SORT_COMPARATOR;

    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_MAPPER_OUTPUT_KEY = MongoConfigUtil.JOB_MAPPER_OUTPUT_KEY;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_MAPPER_OUTPUT_VALUE = MongoConfigUtil.JOB_MAPPER_OUTPUT_VALUE;

    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_INPUT_FORMAT = MongoConfigUtil.JOB_INPUT_FORMAT;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_OUTPUT_FORMAT = MongoConfigUtil.JOB_OUTPUT_FORMAT;

    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_OUTPUT_KEY = MongoConfigUtil.JOB_OUTPUT_KEY;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String JOB_OUTPUT_VALUE = MongoConfigUtil.JOB_OUTPUT_VALUE;

    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_URI = MongoConfigUtil.INPUT_URI;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_MONGOS_HOSTS = MongoConfigUtil.INPUT_MONGOS_HOSTS;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String OUTPUT_URI = MongoConfigUtil.OUTPUT_URI;

    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String MONGO_SPLITTER_CLASS = MongoConfigUtil.MONGO_SPLITTER_CLASS;


    /**
     * <p>
     * The MongoDB field to read from for the Mapper Input.
     * </p>
     * <p>
     * This will be fed to your mapper as the "Key" for the input.
     * </p>
     * <p>
     * Defaults to {@code _id}
     * </p>
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_KEY = MongoConfigUtil.INPUT_KEY;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_NOTIMEOUT = MongoConfigUtil.INPUT_NOTIMEOUT;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_QUERY = MongoConfigUtil.INPUT_QUERY;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_FIELDS = MongoConfigUtil.INPUT_FIELDS;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_SORT = MongoConfigUtil.INPUT_SORT;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_LIMIT = MongoConfigUtil.INPUT_LIMIT;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_SKIP = MongoConfigUtil.INPUT_SKIP;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_LAZY_BSON = MongoConfigUtil.INPUT_LAZY_BSON;


    //Settings specific to bson reading/writing.
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String BSON_SPLITS_PATH = MongoConfigUtil.BSON_SPLITS_PATH;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String BSON_READ_SPLITS = MongoConfigUtil.BSON_READ_SPLITS;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String BSON_WRITE_SPLITS = MongoConfigUtil.BSON_WRITE_SPLITS;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String BSON_OUTPUT_BUILDSPLITS = MongoConfigUtil.BSON_OUTPUT_BUILDSPLITS;
    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String BSON_PATHFILTER = MongoConfigUtil.BSON_PATHFILTER;


    /**
     * <p>
     * A username and password to use.
     * </p>
     * <p>
     * This is necessary when running jobs with a sharded cluster, as access
     * to the config database is needed to get
     * </p>
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String AUTH_URI = MongoConfigUtil.AUTH_URI;


    /**
     * <p>
     * When *not* using 'read_from_shards' or 'read_shard_chunks' The number of megabytes per Split to create for the input data.
     * </p>
     * <p>
     * Currently defaults to 8MB, tweak it as necessary for your code.
     * </p>
     * <p>
     * This default will likely change as we research better options.
     * </p>
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_SPLIT_SIZE = MongoConfigUtil.INPUT_SPLIT_SIZE;

    /**
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final int DEFAULT_SPLIT_SIZE = MongoConfigUtil.DEFAULT_SPLIT_SIZE;

    /**
     * <p>
     * If CREATE_INPUT_SPLITS is true but SPLITS_USE_CHUNKS is false, Mongo-Hadoop will attempt to create custom input splits for you.  By
     * default it will split on {@code _id}, which is a reasonable/sane default.
     * </p>
     * <p>
     * If you want to customize that split point for efficiency reasons (such as different distribution) you may set this to any valid field
     * name. The restriction on this key name are the *exact same rules* as when sharding an existing MongoDB Collection.  You must have an
     * index on the field, and follow the other rules outlined in the docs.
     * </p>
     * <p>
     * This must be a JSON document, and not just a field name!
     * </p>
     *
     * @see <a href="http://docs.mongodb.org/manual/core/sharding-shard-key/">Shard Keys</a>
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String INPUT_SPLIT_KEY_PATTERN = MongoConfigUtil.INPUT_SPLIT_KEY_PATTERN;

    /**
     * <p>
     * If {@code true}, the driver will attempt to split the MongoDB Input data (if reading from Mongo) into multiple InputSplits to allow
     * parallelism/concurrency in processing within Hadoop.  That is to say, Hadoop will assign one InputSplit per mapper.
     * </p>
     * <p>
     * This is {@code true} by default now, but if {@code false}, only one InputSplit (your whole collection) will be assigned to Hadoop,
     * severely reducing parallel mapping.
     * </p>
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String CREATE_INPUT_SPLITS = MongoConfigUtil.CREATE_INPUT_SPLITS;

    /**
     * If {@code true} in a sharded setup splits will be made to connect to individual backend {@code mongod}s.  This can be unsafe. If
     * {@code mongos} is moving chunks around you might see duplicate data, or miss some data entirely.
     *
     * <p>
     * Defaults to {@code false}
     * </p>
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String SPLITS_USE_SHARDS = MongoConfigUtil.SPLITS_USE_SHARDS;
    /**
     * If {@code true} have one split = one shard chunk.  If {@link #SPLITS_USE_SHARDS} is not true splits will still use chunks, but will
     * connect through {@code mongos} instead of the individual backend {@code mongod}s (the safe thing to do). If {@link
     * #SPLITS_USE_SHARDS} is {@code true} but this is {@code false} one split will be made for each backend shard. THIS IS UNSAFE and may
     * result in data being run multiple times
     *
     * <p>
     * Defaults to {@code true }
     * </p>
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String SPLITS_USE_CHUNKS = MongoConfigUtil.SPLITS_USE_CHUNKS;
    /**
     * <p>
     * If true then shards are replica sets run queries on slaves. If set this will override any option passed on the URI.
     * </p>
     * <p>
     * Defaults to {@code false}
     * </p>
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String SPLITS_SLAVE_OK = MongoConfigUtil.SPLITS_SLAVE_OK;

    /**
     * <p>
     * If true then queries for splits will be constructed using $lt/$gt instead of $min and $max.
     * </p>
     * <p>
     * Defaults to {@code false}
     * </p>
     * @deprecated Use the constant in {@link MongoConfigUtil} instead.
     */
    @Deprecated
    public static final String SPLITS_USE_RANGEQUERY = MongoConfigUtil.SPLITS_USE_RANGEQUERY;

    private MapredMongoConfigUtil() {
    }

    public static boolean isJobVerbose(final Configuration conf) {
        return MongoConfigUtil.isJobVerbose(conf);
    }

    public static void setJobVerbose(final Configuration conf, final boolean val) {
        MongoConfigUtil.setJobVerbose(conf, val);
    }

    public static boolean isJobBackground(final Configuration conf) {
        return MongoConfigUtil.isJobBackground(conf);
    }

    public static void setJobBackground(final Configuration conf, final boolean val) {
        MongoConfigUtil.setJobBackground(conf, val);
    }

    // TODO - In light of key/value specifics should we have a base MongoMapper
    // class?
    public static Class<? extends Mapper> getMapper(final Configuration conf) {
        /** TODO - Support multiple inputs via getClasses ? **/
        return conf.getClass(MongoConfigUtil.JOB_MAPPER, null, Mapper.class);
    }

    public static void setMapper(final Configuration conf, final Class<? extends Mapper> val) {
        conf.setClass(MongoConfigUtil.JOB_MAPPER, val, Mapper.class);
    }

    public static Class<?> getMapperOutputKey(final Configuration conf) {
        return MongoConfigUtil.getMapperOutputKey(conf);
    }

    public static void setMapperOutputKey(final Configuration conf, final Class<?> val) {
        MongoConfigUtil.setMapperOutputKey(conf, val);
    }

    public static Class<?> getMapperOutputValue(final Configuration conf) {
        return MongoConfigUtil.getMapperOutputValue(conf);
    }

    public static void setMapperOutputValue(final Configuration conf, final Class<?> val) {
        MongoConfigUtil.setMapperOutputValue(conf, val);
    }

    public static Class<? extends Reducer> getCombiner(final Configuration conf) {
        return conf.getClass(MongoConfigUtil.JOB_COMBINER, null, Reducer.class);
    }

    public static void setCombiner(final Configuration conf, final Class<? extends Reducer> val) {
        conf.setClass(MongoConfigUtil.JOB_COMBINER, val, Reducer.class);
    }

    // TODO - In light of key/value specifics should we have a base MongoReducer
    // class?
    public static Class<? extends Reducer> getReducer(final Configuration conf) {
        /** TODO - Support multiple outputs via getClasses ? **/
        return conf.getClass(MongoConfigUtil.JOB_REDUCER, null, Reducer.class);
    }

    public static void setReducer(final Configuration conf, final Class<? extends Reducer> val) {
        conf.setClass(MongoConfigUtil.JOB_REDUCER, val, Reducer.class);
    }

    public static Class<? extends Partitioner> getPartitioner(final Configuration conf) {
        return conf.getClass(MongoConfigUtil.JOB_PARTITIONER, null, Partitioner.class);
    }

    public static void setPartitioner(final Configuration conf, final Class<? extends Partitioner> val) {
        conf.setClass(MongoConfigUtil.JOB_PARTITIONER, val, Partitioner.class);
    }

    public static Class<? extends RawComparator> getSortComparator(final Configuration conf) {
        return MongoConfigUtil.getSortComparator(conf);
    }

    public static void setSortComparator(final Configuration conf, final Class<? extends RawComparator> val) {
        MongoConfigUtil.setSortComparator(conf, val);
    }

    public static Class<? extends OutputFormat> getOutputFormat(final Configuration conf) {
        return conf.getClass(MongoConfigUtil.JOB_OUTPUT_FORMAT, null, OutputFormat.class);
    }

    public static void setOutputFormat(final Configuration conf, final Class<? extends OutputFormat> val) {
        conf.setClass(MongoConfigUtil.JOB_OUTPUT_FORMAT, val, OutputFormat.class);
    }

    public static Class<?> getOutputKey(final Configuration conf) {
        return MongoConfigUtil.getOutputKey(conf);
    }

    public static void setOutputKey(final Configuration conf, final Class<?> val) {
        MongoConfigUtil.setOutputKey(conf, val);
    }

    public static Class<?> getOutputValue(final Configuration conf) {
        return MongoConfigUtil.getOutputValue(conf);
    }

    public static void setOutputValue(final Configuration conf, final Class<?> val) {
        MongoConfigUtil.setOutputValue(conf, val);
    }

    public static Class<? extends InputFormat> getInputFormat(final Configuration conf) {
        return conf.getClass(MongoConfigUtil.JOB_INPUT_FORMAT, null, InputFormat.class);
    }

    public static void setInputFormat(final Configuration conf, final Class<? extends InputFormat> val) {
        conf.setClass(MongoConfigUtil.JOB_INPUT_FORMAT, val, InputFormat.class);
    }

    public static List<MongoClientURI> getMongoURIs(final Configuration conf, final String key) {
        return MongoConfigUtil.getMongoURIs(conf, key);
    }

    /**
     * @deprecated use {@link #getMongoClientURI(Configuration, String)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static MongoURI getMongoURI(final Configuration conf, final String key) {
        return MongoConfigUtil.getMongoURI(conf, key);
    }

    public static MongoClientURI getMongoClientURI(final Configuration conf, final String key) {
        return MongoConfigUtil.getMongoClientURI(conf, key);
    }

    public static MongoClientURI getInputURI(final Configuration conf) {
        return MongoConfigUtil.getInputURI(conf);
    }

    public static MongoClientURI getAuthURI(final Configuration conf) {
        return MongoConfigUtil.getAuthURI(conf);
    }

    public static List<DBCollection> getCollections(final List<MongoClientURI> uris, final MongoClientURI authURI) {
        return MongoConfigUtil.getCollections(uris, authURI);
    }

    /**
     * @deprecated use {@link #getCollection(MongoClientURI)}
     */
    @Deprecated
    public static DBCollection getCollection(final MongoURI uri) {
        return MongoConfigUtil.getCollection(uri);
    }

    public static DBCollection getCollection(final MongoClientURI uri) {
        return MongoConfigUtil.getCollection(uri);
    }

    /**
     * @deprecated use {@link #getCollectionWithAuth(MongoClientURI, MongoClientURI)} instead
     */
    @Deprecated
    public static DBCollection getCollectionWithAuth(final MongoURI uri, final MongoURI authURI) {
        return MongoConfigUtil.getCollectionWithAuth(uri, authURI);
    }

    public static DBCollection getCollectionWithAuth(final MongoClientURI uri, final MongoClientURI authURI) {
        return MongoConfigUtil.getCollectionWithAuth(uri, authURI);
    }

    public static DBCollection getOutputCollection(final Configuration conf) {
        return MongoConfigUtil.getOutputCollection(conf);
    }

    public static List<DBCollection> getOutputCollections(final Configuration conf) {
        return MongoConfigUtil.getOutputCollections(conf);
    }

    public static DBCollection getInputCollection(final Configuration conf) {
        return MongoConfigUtil.getInputCollection(conf);
    }

    /**
     * @deprecated use {@link #setMongoURI(Configuration, String, MongoClientURI)} instead
     */
    @Deprecated
    public static void setMongoURI(final Configuration conf, final String key, final MongoURI value) {
        MongoConfigUtil.setMongoURI(conf, key, value);
    }

    public static void setMongoURI(final Configuration conf, final String key, final MongoClientURI value) {
        MongoConfigUtil.setMongoURI(conf, key, value);
    }

    public static void setMongoURIString(final Configuration conf, final String key, final String value) {
        MongoConfigUtil.setMongoURIString(conf, key, value);
    }

    public static void setAuthURI(final Configuration conf, final String uri) {
        MongoConfigUtil.setAuthURI(conf, uri);
    }

    public static void setInputURI(final Configuration conf, final String uri) {
        MongoConfigUtil.setInputURI(conf, uri);
    }

    /**
     * @deprecated use {@link #setInputURI(Configuration, MongoClientURI)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static void setInputURI(final Configuration conf, final MongoURI uri) {
        MongoConfigUtil.setInputURI(conf, uri);
    }

    public static void setInputURI(final Configuration conf, final MongoClientURI uri) {
        MongoConfigUtil.setInputURI(conf, uri);
    }

    public static List<MongoClientURI> getOutputURIs(final Configuration conf) {
        return MongoConfigUtil.getOutputURIs(conf);
    }

    public static MongoClientURI getOutputURI(final Configuration conf) {
        return MongoConfigUtil.getOutputURI(conf);
    }

    public static void setOutputURI(final Configuration conf, final String uri) {
        MongoConfigUtil.setOutputURI(conf, uri);
    }
     /**
     * @deprecated use {@link #setOutputURI(Configuration, MongoClientURI)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static void setOutputURI(final Configuration conf, final MongoURI uri) {
        MongoConfigUtil.setOutputURI(conf, uri);
    }

    public static void setOutputURI(final Configuration conf, final MongoClientURI uri) {
        MongoConfigUtil.setOutputURI(conf, uri);
    }

    /**
     * Set JSON but first validate it's parsable into a DBObject
     */
    public static void setJSON(final Configuration conf, final String key, final String value) {
        MongoConfigUtil.setJSON(conf, key, value);
    }

    public static DBObject getDBObject(final Configuration conf, final String key) {
        return MongoConfigUtil.getDBObject(conf, key);
    }

    public static void setDBObject(final Configuration conf, final String key, final DBObject value) {
        MongoConfigUtil.setDBObject(conf, key, value);
    }

    public static void setQuery(final Configuration conf, final String query) {
        MongoConfigUtil.setQuery(conf, query);
    }

    public static void setQuery(final Configuration conf, final DBObject query) {
        MongoConfigUtil.setQuery(conf, query);
    }

    /**
     * Returns the configured query as a DBObject... If you want a string call toString() on the returned object. or use JSON.serialize()
     */
    public static DBObject getQuery(final Configuration conf) {
        return MongoConfigUtil.getQuery(conf);
    }

    public static void setFields(final Configuration conf, final String fields) {
        MongoConfigUtil.setFields(conf, fields);
    }

    public static void setFields(final Configuration conf, final DBObject fields) {
        MongoConfigUtil.setFields(conf, fields);
    }

    /**
     * Returns the configured fields as a DBObject... If you want a string call toString() on the returned object. or use JSON.serialize()
     */
    public static DBObject getFields(final Configuration conf) {
        return MongoConfigUtil.getFields(conf);
    }

    public static void setSort(final Configuration conf, final String sort) {
        MongoConfigUtil.setSort(conf, sort);
    }

    public static void setSort(final Configuration conf, final DBObject sort) {
        MongoConfigUtil.setSort(conf, sort);
    }

    /**
     * Returns the configured sort as a DBObject... If you want a string call toString() on the returned object. or use JSON.serialize()
     */
    public static DBObject getSort(final Configuration conf) {
        return MongoConfigUtil.getSort(conf);
    }

    public static int getLimit(final Configuration conf) {
        return MongoConfigUtil.getLimit(conf);
    }

    public static void setLimit(final Configuration conf, final int limit) {
        MongoConfigUtil.setLimit(conf, limit);
    }

    public static int getSkip(final Configuration conf) {
        return MongoConfigUtil.getSkip(conf);
    }

    public static void setSkip(final Configuration conf, final int skip) {
        MongoConfigUtil.setSkip(conf, skip);
    }

    public static boolean getLazyBSON(final Configuration conf) {
        return MongoConfigUtil.getLazyBSON(conf);
    }

    public static void setLazyBSON(final Configuration conf, final boolean lazy) {
        MongoConfigUtil.setLazyBSON(conf, lazy);
    }

    public static int getSplitSize(final Configuration conf) {
        return MongoConfigUtil.getSplitSize(conf);
    }

    public static void setSplitSize(final Configuration conf, final int value) {
        MongoConfigUtil.setSplitSize(conf, value);
    }

    /**
     * if TRUE, Splits will be queried using $lt/$gt instead of $max and $min. This allows the database's query optimizer to choose the best
     * index, instead of being forced to use the one in the $max/$min keys. This will only work if the key used for splitting is *not* a
     * compound key. Make sure that all values under the splitting key are of the same type, or this will cause incomplete results.
     */
    public static boolean isRangeQueryEnabled(final Configuration conf) {
        return conf.getBoolean(MongoConfigUtil.SPLITS_USE_RANGEQUERY, false);
    }

    public static void setRangeQueryEnabled(final Configuration conf, final boolean value) {
        MongoConfigUtil.setRangeQueryEnabled(conf, value);
    }

    /**
     * if TRUE, Splits will be read by connecting to the individual shard servers, Only use this ( issue has to do with chunks moving /
     * relocating during balancing phases)
     */
    public static boolean canReadSplitsFromShards(final Configuration conf) {
        return MongoConfigUtil.canReadSplitsFromShards(conf);
    }

    public static void setReadSplitsFromShards(final Configuration conf, final boolean value) {
        MongoConfigUtil.setReadSplitsFromShards(conf, value);
    }

    /**
     * If sharding is enabled, Use the sharding configured chunks to split up data.
     */
    public static boolean isShardChunkedSplittingEnabled(final Configuration conf) {
        return MongoConfigUtil.isShardChunkedSplittingEnabled(conf);
    }

    public static void setShardChunkSplittingEnabled(final Configuration conf, final boolean value) {
        MongoConfigUtil.setShardChunkSplittingEnabled(conf, value);
    }

    public static boolean canReadSplitsFromSecondary(final Configuration conf) {
        return MongoConfigUtil.canReadSplitsFromSecondary(conf);
    }

    public static void setReadSplitsFromSecondary(final Configuration conf, final boolean value) {
        MongoConfigUtil.setReadSplitsFromSecondary(conf, value);
    }

    public static boolean createInputSplits(final Configuration conf) {
        return MongoConfigUtil.createInputSplits(conf);
    }

    public static void setCreateInputSplits(final Configuration conf, final boolean value) {
        MongoConfigUtil.setCreateInputSplits(conf, value);
    }

    public static void setInputSplitKeyPattern(final Configuration conf, final String pattern) {
        MongoConfigUtil.setInputSplitKeyPattern(conf, pattern);
    }

    public static void setInputSplitKey(final Configuration conf, final DBObject key) {
        MongoConfigUtil.setInputSplitKey(conf, key);
    }

    public static String getInputSplitKeyPattern(final Configuration conf) {
        return MongoConfigUtil.getInputSplitKeyPattern(conf);
    }

    public static DBObject getInputSplitKey(final Configuration conf) {
        return MongoConfigUtil.getInputSplitKey(conf);
    }


    public static void setInputKey(final Configuration conf, final String fieldName) {
        MongoConfigUtil.setInputKey(conf, fieldName);
    }

    public static String getInputKey(final Configuration conf) {
        return MongoConfigUtil.getInputKey(conf);
    }

    public static void setNoTimeout(final Configuration conf, final boolean value) {
        MongoConfigUtil.setNoTimeout(conf, value);
    }

    public static boolean isNoTimeout(final Configuration conf) {
        return MongoConfigUtil.isNoTimeout(conf);
    }

    //BSON-specific config functions.
    public static boolean getBSONReadSplits(final Configuration conf) {
        return MongoConfigUtil.getBSONReadSplits(conf);
    }

    public static void setBSONReadSplits(final Configuration conf, final boolean val) {
        MongoConfigUtil.setBSONReadSplits(conf, val);
    }

    public static boolean getBSONWriteSplits(final Configuration conf) {
        return MongoConfigUtil.getBSONWriteSplits(conf);
    }

    public static void setBSONWriteSplits(final Configuration conf, final boolean val) {
        MongoConfigUtil.setBSONWriteSplits(conf, val);
    }

    public static boolean getBSONOutputBuildSplits(final Configuration conf) {
        return MongoConfigUtil.getBSONOutputBuildSplits(conf);
    }

    public static void setBSONOutputBuildSplits(final Configuration conf, final boolean val) {
        MongoConfigUtil.setBSONOutputBuildSplits(conf, val);
    }

    public static String getBSONSplitsPath(final Configuration conf) {
        return MongoConfigUtil.getBSONSplitsPath(conf);
    }

    public static void setBSONSplitsPath(final Configuration conf,
                                         final String path) {
        MongoConfigUtil.setBSONSplitsPath(conf, path);
    }

    public static void setBSONPathFilter(final Configuration conf, final Class<? extends PathFilter> val) {
        MongoConfigUtil.setBSONPathFilter(conf, val);
    }

    public static Class<?> getBSONPathFilter(final Configuration conf) {
        return MongoConfigUtil.getBSONPathFilter(conf);
    }

    public static Class<? extends MongoSplitter> getSplitterClass(final Configuration conf) {
        return MongoConfigUtil.getSplitterClass(conf);
    }

    public static void setSplitterClass(final Configuration conf, final Class<? extends MongoSplitter> val) {
        MongoConfigUtil.setSplitterClass(conf, val);
    }

    public static List<String> getInputMongosHosts(final Configuration conf) {
        return MongoConfigUtil.getInputMongosHosts(conf);
    }

    public static void setInputMongosHosts(final Configuration conf, final List<String> hostnames) {
        MongoConfigUtil.setInputMongosHosts(conf, hostnames);
    }

    /**
     * Fetch a class by its actual class name, rather than by a key name in the configuration properties. We still need to pass in a
     * Configuration object here, since the Configuration class maintains an internal cache of class names for performance on some hadoop
     * versions. It also ensures that the same classloader is used across all keys.
     */
    public static <U> Class<? extends U> getClassByName(final Configuration conf,
                                                        final String className,
                                                        final Class<U> xface) {
        return MongoConfigUtil.getClassByName(conf, className, xface);
    }

    public static Configuration buildConfiguration(final Map<String, Object> data) {
        return MongoConfigUtil.buildConfiguration(data);
    }

}
