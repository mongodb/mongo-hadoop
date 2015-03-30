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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.util.JSON;
import org.apache.commons.lang.StringUtils;
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

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Configuration helper tool for MongoDB related Map/Reduce jobs
 */

public final class MongoConfigUtil {
    private static final Log LOG = LogFactory.getLog(MongoConfigUtil.class);

    /**
     * The JOB_* values are entirely optional and disregarded unless you use the MongoTool base toolset... If you don't, feel free to ignore
     * these
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
    public static final String INPUT_MONGOS_HOSTS = "mongo.input.mongos_hosts";
    public static final String OUTPUT_URI = "mongo.output.uri";

    public static final String MONGO_SPLITTER_CLASS = "mongo.splitter.class";


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
     */
    public static final String INPUT_KEY = "mongo.input.key";
    public static final String INPUT_NOTIMEOUT = "mongo.input.notimeout";
    public static final String INPUT_QUERY = "mongo.input.query";
    public static final String INPUT_FIELDS = "mongo.input.fields";
    public static final String INPUT_SORT = "mongo.input.sort";
    public static final String INPUT_LIMIT = "mongo.input.limit";
    public static final String INPUT_SKIP = "mongo.input.skip";
    public static final String INPUT_LAZY_BSON = "mongo.input.lazy_bson";


    //Settings specific to bson reading/writing.
    public static final String BSON_SPLITS_PATH = "bson.split.splits_path";
    public static final String BSON_READ_SPLITS = "bson.split.read_splits";
    public static final String BSON_WRITE_SPLITS = "bson.split.write_splits";
    public static final String BSON_OUTPUT_BUILDSPLITS = "bson.output.build_splits";
    public static final String BSON_PATHFILTER = "bson.pathfilter.class";


    /**
     * <p>
     * A username and password to use.
     * </p>
     * <p>
     * This is necessary when running jobs with a sharded cluster, as access to the config database is needed to get
     * </p>
     */
    public static final String AUTH_URI = "mongo.auth.uri";


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
     */
    public static final String INPUT_SPLIT_SIZE = "mongo.input.split_size";

    public static final int DEFAULT_SPLIT_SIZE = 8; // 8 mb per manual (non-sharding) split

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
     */
    public static final String INPUT_SPLIT_KEY_PATTERN = "mongo.input.split.split_key_pattern";

    /**
     * <p>
     * If {@code true}, the driver will attempt to split the MongoDB Input data (if reading from Mongo) into multiple InputSplits to allow
     * parallelism/concurrency in processing within Hadoop.  That is to say, Hadoop will assign one InputSplit per mapper.
     * </p>
     * <p>
     * This is {@code true} by default now, but if {@code false}, only one InputSplit (your whole collection) will be assigned to Hadoop,
     * severely reducing parallel mapping.
     * </p>
     */
    public static final String CREATE_INPUT_SPLITS = "mongo.input.split.create_input_splits";

    /**
     * If {@code true} in a sharded setup splits will be made to connect to individual backend {@code mongod}s.  This can be unsafe. If
     * {@code mongos} is moving chunks around you might see duplicate data, or miss some data entirely. Defaults to {@code false}
     */
    public static final String SPLITS_USE_SHARDS = "mongo.input.split.read_from_shards";
    /**
     * If {@code true} have one split = one shard chunk.  If {@link #SPLITS_USE_SHARDS} is not true splits will still use chunks, but will
     * connect through {@code mongos} instead of the individual backend {@code mongod}s (the safe thing to do). If {@link
     * #SPLITS_USE_SHARDS} is {@code true} but this is {@code false} one split will be made for each backend shard. THIS IS UNSAFE and may
     * result in data being run multiple times <p> Defaults to {@code true }
     */
    public static final String SPLITS_USE_CHUNKS = "mongo.input.split.read_shard_chunks";
    /**
     * <p>
     * If true then shards are replica sets run queries on slaves. If set this will override any option passed on the URI.
     * </p>
     * <p>
     * Defaults to {@code false}
     * </p>
     */
    public static final String SPLITS_SLAVE_OK = "mongo.input.split.allow_read_from_secondaries";

    /**
     * <p>
     * If true then queries for splits will be constructed using $lt/$gt instead of $min and $max.
     * </p>
     * <p>
     * Defaults to {@code false}
     * </p>
     */
    public static final String SPLITS_USE_RANGEQUERY = "mongo.input.split.use_range_queries";

    /**
     * The lower bound shard key to use when creating the input splits.<p/>
     * Defaults to {@code null}.  Values must be provided for both this key
     * and {@link #SPLITS_MAX_KEY} in order for the range to be used.  Remember
     * that if your index is ordered descending then the value for this key
     * will actually be greater than the value specified at {@link #SPLITS_MAX_KEY}.
     **/
    public static final String SPLITS_MIN_KEY = "mongo.input.split.split_key_min";

    /**
     * The upper bound shard key to use when creating the input splits.<p/>
     * Defaults to {@code null}.  Values must be provided for both this key
     * and {@link #SPLITS_MIN_KEY} in order for the range to be used.  Remember
     * that if your index is ordered descending then the value for this key
     * will actually be less than the value specified at {@link #SPLITS_MIN_KEY}.
     **/
    public static final String SPLITS_MAX_KEY = "mongo.input.split.split_key_max";

    /**
     * Shared MongoClient instance cache.
     */
    private static final Map<MongoClientURI, MongoClient> CLIENTS = new HashMap<MongoClientURI, MongoClient>();
    
    private static Map<MongoClient, MongoClientURI> uriMap = new HashMap<MongoClient, MongoClientURI>();

    private MongoConfigUtil() {
    }

    public static boolean isJobVerbose(final Configuration conf) {
        return conf.getBoolean(JOB_VERBOSE, false);
    }

    public static void setJobVerbose(final Configuration conf, final boolean val) {
        conf.setBoolean(JOB_VERBOSE, val);
    }

    public static boolean isJobBackground(final Configuration conf) {
        return conf.getBoolean(JOB_BACKGROUND, false);
    }

    public static void setJobBackground(final Configuration conf, final boolean val) {
        conf.setBoolean(JOB_BACKGROUND, val);
    }

    // TODO - In light of key/value specifics should we have a base MongoMapper
    // class?
    public static Class<? extends Mapper> getMapper(final Configuration conf) {
        /** TODO - Support multiple inputs via getClasses ? **/
        return conf.getClass(JOB_MAPPER, null, Mapper.class);
    }

    public static void setMapper(final Configuration conf, final Class<? extends Mapper> val) {
        conf.setClass(JOB_MAPPER, val, Mapper.class);
    }

    public static Class<?> getMapperOutputKey(final Configuration conf) {
        return conf.getClass(JOB_MAPPER_OUTPUT_KEY, null);
    }

    public static void setMapperOutputKey(final Configuration conf, final Class<?> val) {
        conf.setClass(JOB_MAPPER_OUTPUT_KEY, val, Object.class);
    }

    public static Class<?> getMapperOutputValue(final Configuration conf) {
        return conf.getClass(JOB_MAPPER_OUTPUT_VALUE, null);
    }

    public static void setMapperOutputValue(final Configuration conf, final Class<?> val) {
        conf.setClass(JOB_MAPPER_OUTPUT_VALUE, val, Object.class);
    }

    public static Class<? extends Reducer> getCombiner(final Configuration conf) {
        return conf.getClass(JOB_COMBINER, null, Reducer.class);
    }

    public static void setCombiner(final Configuration conf, final Class<? extends Reducer> val) {
        conf.setClass(JOB_COMBINER, val, Reducer.class);
    }

    // TODO - In light of key/value specifics should we have a base MongoReducer
    // class?
    public static Class<? extends Reducer> getReducer(final Configuration conf) {
        /** TODO - Support multiple outputs via getClasses ? **/
        return conf.getClass(JOB_REDUCER, null, Reducer.class);
    }

    public static void setReducer(final Configuration conf, final Class<? extends Reducer> val) {
        conf.setClass(JOB_REDUCER, val, Reducer.class);
    }

    public static Class<? extends Partitioner> getPartitioner(final Configuration conf) {
        return conf.getClass(JOB_PARTITIONER, null, Partitioner.class);
    }

    public static void setPartitioner(final Configuration conf, final Class<? extends Partitioner> val) {
        conf.setClass(JOB_PARTITIONER, val, Partitioner.class);
    }

    public static Class<? extends RawComparator> getSortComparator(final Configuration conf) {
        return conf.getClass(JOB_SORT_COMPARATOR, null, RawComparator.class);
    }

    public static void setSortComparator(final Configuration conf, final Class<? extends RawComparator> val) {
        conf.setClass(JOB_SORT_COMPARATOR, val, RawComparator.class);
    }

    public static Class<? extends OutputFormat> getOutputFormat(final Configuration conf) {
        return conf.getClass(JOB_OUTPUT_FORMAT, null, OutputFormat.class);
    }

    public static void setOutputFormat(final Configuration conf, final Class<? extends OutputFormat> val) {
        conf.setClass(JOB_OUTPUT_FORMAT, val, OutputFormat.class);
    }

    public static Class<?> getOutputKey(final Configuration conf) {
        return conf.getClass(JOB_OUTPUT_KEY, null);
    }

    public static void setOutputKey(final Configuration conf, final Class<?> val) {
        conf.setClass(JOB_OUTPUT_KEY, val, Object.class);
    }

    public static Class<?> getOutputValue(final Configuration conf) {
        return conf.getClass(JOB_OUTPUT_VALUE, null);
    }

    public static void setOutputValue(final Configuration conf, final Class<?> val) {
        conf.setClass(JOB_OUTPUT_VALUE, val, Object.class);
    }

    public static Class<? extends InputFormat> getInputFormat(final Configuration conf) {
        return conf.getClass(JOB_INPUT_FORMAT, null, InputFormat.class);
    }

    public static void setInputFormat(final Configuration conf, final Class<? extends InputFormat> val) {
        conf.setClass(JOB_INPUT_FORMAT, val, InputFormat.class);
    }

    public static List<MongoClientURI> getMongoURIs(final Configuration conf, final String key) {
        final String raw = conf.get(key);
        if (raw != null && !raw.trim().isEmpty()) {
            List<MongoClientURI> result = new LinkedList<MongoClientURI>();
            String[] split = StringUtils.split(raw, ", ");
            for (String mongoURI : split) {
                result.add(new MongoClientURI(mongoURI));
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * @deprecated use {@link #getMongoClientURI(Configuration, String)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static MongoURI getMongoURI(final Configuration conf, final String key) {
        final String raw = conf.get(key);
        if (raw != null && !raw.trim().isEmpty()) {
            return new MongoURI(raw);
        } else {
            return null;
        }
    }
    
    public static MongoClientURI getMongoClientURI(final Configuration conf, final String key) {
        final String raw = conf.get(key);
        return raw != null && !raw.trim().isEmpty() ? new MongoClientURI(raw) : null;
    }

    public static MongoClientURI getInputURI(final Configuration conf) {
        return getMongoClientURI(conf, INPUT_URI);
    }

    public static MongoClientURI getAuthURI(final Configuration conf) {
        return getMongoClientURI(conf, AUTH_URI);
    }

    public static List<DBCollection> getCollections(final List<MongoClientURI> uris, final MongoClientURI authURI) {
        List<DBCollection> dbCollections = new LinkedList<DBCollection>();
        for (MongoClientURI uri : uris) {
            if (authURI != null) {
                dbCollections.add(getCollectionWithAuth(uri, authURI));
            } else {
                dbCollections.add(getCollection(uri));
            }
        }
        return dbCollections;
    }

    /**
     * @deprecated use {@link #getCollection(MongoClientURI)}
     */
    @Deprecated
    public static DBCollection getCollection(final MongoURI uri) {
        return getCollection(new MongoClientURI(uri.toString()));
    }
    
    public static DBCollection getCollection(final MongoClientURI uri) {
        try {
            return getMongoClient(uri).getDB(uri.getDatabase()).getCollection(uri.getCollection());
        } catch (Exception e) {
            throw new IllegalArgumentException("Couldn't connect and authenticate to get collection", e);
        }
    }

    /**
     * @deprecated use {@link #getCollectionWithAuth(MongoClientURI, MongoClientURI)} instead
     */
    @Deprecated
    public static DBCollection getCollectionWithAuth(final MongoURI uri, final MongoURI authURI) {
        return getCollectionWithAuth(new MongoClientURI(uri.toString()), new MongoClientURI(authURI.toString()));
    }
    
    public static DBCollection getCollectionWithAuth(final MongoClientURI uri, final MongoClientURI authURI) {
        //Make sure auth uri is valid and actually has a username/pw to use
        if (authURI == null || authURI.getUsername() == null || authURI.getPassword() == null) {
            throw new IllegalArgumentException("auth URI is empty or does not contain a valid username/password combination.");
        }

        DBCollection coll;
        try {
            Mongo mongo = getMongoClient(authURI);
            coll = mongo.getDB(uri.getDatabase()).getCollection(uri.getCollection());
            return coll;
        } catch (Exception e) {
            throw new IllegalArgumentException("Couldn't connect and authenticate to get collection", e);
        }
    }

    public static DBCollection getOutputCollection(final Configuration conf) {
        try {
            return getCollection(getOutputURI(conf));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Unable to connect to MongoDB Output Collection.", e);
        }
    }

    public static List<DBCollection> getOutputCollections(final Configuration conf) {
        try {
            return getCollections(getOutputURIs(conf), getAuthURI(conf));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Unable to connect to MongoDB Output Collection.", e);
        }
    }

    public static DBCollection getInputCollection(final Configuration conf) {
        try {
            return getCollection(getInputURI(conf));
        } catch (final Exception e) {
            throw new IllegalArgumentException(
                                                  "Unable to connect to MongoDB Input Collection at '" + getInputURI(conf) + "'", e);
        }
    }

    /**
     * @deprecated use {@link #setMongoURI(Configuration, String, MongoClientURI)} instead
     */
    @Deprecated
    public static void setMongoURI(final Configuration conf, final String key, final MongoURI value) {
        conf.set(key, value.toString()); // todo - verify you can toString a
        // URI object
    }

    public static void setMongoURI(final Configuration conf, final String key, final MongoClientURI value) {
        conf.set(key, value.toString()); // todo - verify you can toString a
        // URI object
    }

    public static void setMongoURIString(final Configuration conf, final String key, final String value) {
        setMongoURI(conf, key, new MongoClientURI(value));
    }

    public static void setAuthURI(final Configuration conf, final String uri) {
        setMongoURIString(conf, AUTH_URI, uri);
    }

    public static void setInputURI(final Configuration conf, final String uri) {
        setMongoURIString(conf, INPUT_URI, uri);
    }

    /**
     * @deprecated use {@link #setInputURI(Configuration, MongoClientURI)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static void setInputURI(final Configuration conf, final MongoURI uri) {
        setMongoURI(conf, INPUT_URI, uri);
    }

    public static void setInputURI(final Configuration conf, final MongoClientURI uri) {
        setMongoURI(conf, INPUT_URI, uri);
    }

    public static List<MongoClientURI> getOutputURIs(final Configuration conf) {
        return getMongoURIs(conf, OUTPUT_URI);
    }

    public static MongoClientURI getOutputURI(final Configuration conf) {
        return getMongoClientURI(conf, OUTPUT_URI);
    }

    public static void setOutputURI(final Configuration conf, final String uri) {
        setMongoURIString(conf, OUTPUT_URI, uri);
    }
     /**
     * @deprecated use {@link #setOutputURI(Configuration, MongoClientURI)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static void setOutputURI(final Configuration conf, final MongoURI uri) {
        setMongoURI(conf, OUTPUT_URI, uri);
    }

    public static void setOutputURI(final Configuration conf, final MongoClientURI uri) {
        setMongoURI(conf, OUTPUT_URI, uri);
    }

    /**
     * Set JSON but first validate it's parsable into a DBObject
     */
    public static void setJSON(final Configuration conf, final String key, final String value) {
        try {
            final Object dbObj = JSON.parse(value);
            setDBObject(conf, key, (DBObject) dbObj);
        } catch (final Exception e) {
            LOG.error("Cannot parse JSON...", e);
            throw new IllegalArgumentException("Provided JSON String is not representable/parseable as a DBObject.",
                                               e);
        }
    }

    public static DBObject getDBObject(final Configuration conf, final String key) {
        try {
            final String json = conf.get(key);
            final DBObject obj = (DBObject) JSON.parse(json);
            if (obj == null) {
                return new BasicDBObject();
            } else {
                return obj;
            }
        } catch (final Exception e) {
            throw new IllegalArgumentException("Provided JSON String is not representable/parseable as a DBObject.",
                                               e);
        }
    }

    public static void setDBObject(final Configuration conf, final String key, final DBObject value) {
        conf.set(key, JSON.serialize(value));
    }

    public static void setQuery(final Configuration conf, final String query) {
        setJSON(conf, INPUT_QUERY, query);
    }

    public static void setQuery(final Configuration conf, final DBObject query) {
        setDBObject(conf, INPUT_QUERY, query);
    }

    /**
     * Returns the configured query as a DBObject... If you want a string call toString() on the returned object. or use JSON.serialize()
     */
    public static DBObject getQuery(final Configuration conf) {
        return getDBObject(conf, INPUT_QUERY);
    }

    public static void setFields(final Configuration conf, final String fields) {
        setJSON(conf, INPUT_FIELDS, fields);
    }

    public static void setFields(final Configuration conf, final DBObject fields) {
        setDBObject(conf, INPUT_FIELDS, fields);
    }

    /**
     * Returns the configured fields as a DBObject... If you want a string call toString() on the returned object. or use JSON.serialize()
     */
    public static DBObject getFields(final Configuration conf) {
        return getDBObject(conf, INPUT_FIELDS);
    }

    public static void setSort(final Configuration conf, final String sort) {
        setJSON(conf, INPUT_SORT, sort);
    }

    public static void setSort(final Configuration conf, final DBObject sort) {
        setDBObject(conf, INPUT_SORT, sort);
    }

    /**
     * Returns the configured sort as a DBObject... If you want a string call toString() on the returned object. or use JSON.serialize()
     */
    public static DBObject getSort(final Configuration conf) {
        return getDBObject(conf, INPUT_SORT);
    }

    public static int getLimit(final Configuration conf) {
        return conf.getInt(INPUT_LIMIT, 0);
    }

    public static void setLimit(final Configuration conf, final int limit) {
        conf.setInt(INPUT_LIMIT, limit);
    }

    public static int getSkip(final Configuration conf) {
        return conf.getInt(INPUT_SKIP, 0);
    }

    public static void setSkip(final Configuration conf, final int skip) {
        conf.setInt(INPUT_SKIP, skip);
    }

    public static boolean getLazyBSON(final Configuration conf) {
        return conf.getBoolean(INPUT_LAZY_BSON, false);
    }

    public static void setLazyBSON(final Configuration conf, final boolean lazy) {
        conf.setBoolean(INPUT_LAZY_BSON, lazy);
    }

    public static int getSplitSize(final Configuration conf) {
        return conf.getInt(INPUT_SPLIT_SIZE, DEFAULT_SPLIT_SIZE);
    }

    public static void setSplitSize(final Configuration conf, final int value) {
        conf.setInt(INPUT_SPLIT_SIZE, value);
    }

    /**
     * if TRUE, Splits will be queried using $lt/$gt instead of $max and $min. This allows the database's query optimizer to choose the best
     * index, instead of being forced to use the one in the $max/$min keys. This will only work if the key used for splitting is *not* a
     * compound key. Make sure that all values under the splitting key are of the same type, or this will cause incomplete results.
     */
    public static boolean isRangeQueryEnabled(final Configuration conf) {
        return conf.getBoolean(SPLITS_USE_RANGEQUERY, false);
    }

    public static void setRangeQueryEnabled(final Configuration conf, final boolean value) {
        conf.setBoolean(SPLITS_USE_RANGEQUERY, value);
    }

    /**
     * if TRUE, Splits will be read by connecting to the individual shard servers, Only use this ( issue has to do with chunks moving /
     * relocating during balancing phases)
     */
    public static boolean canReadSplitsFromShards(final Configuration conf) {
        return conf.getBoolean(SPLITS_USE_SHARDS, false);
    }

    public static void setReadSplitsFromShards(final Configuration conf, final boolean value) {
        conf.setBoolean(SPLITS_USE_SHARDS, value);
    }

    /**
     * If sharding is enabled, Use the sharding configured chunks to split up data.
     */
    public static boolean isShardChunkedSplittingEnabled(final Configuration conf) {
        return conf.getBoolean(SPLITS_USE_CHUNKS, true);
    }

    public static void setShardChunkSplittingEnabled(final Configuration conf, final boolean value) {
        conf.setBoolean(SPLITS_USE_CHUNKS, value);
    }

    public static boolean canReadSplitsFromSecondary(final Configuration conf) {
        return conf.getBoolean(SPLITS_SLAVE_OK, false);
    }

    public static void setReadSplitsFromSecondary(final Configuration conf, final boolean value) {
        conf.getBoolean(SPLITS_SLAVE_OK, value);
    }

    public static boolean createInputSplits(final Configuration conf) {
        return conf.getBoolean(CREATE_INPUT_SPLITS, true);
    }

    public static void setCreateInputSplits(final Configuration conf, final boolean value) {
        conf.setBoolean(CREATE_INPUT_SPLITS, value);
    }

    public static void setInputSplitKeyPattern(final Configuration conf, final String pattern) {
        setJSON(conf, INPUT_SPLIT_KEY_PATTERN, pattern);
    }

    public static void setInputSplitKey(final Configuration conf, final DBObject key) {
        setDBObject(conf, INPUT_SPLIT_KEY_PATTERN, key);
    }

    public static String getInputSplitKeyPattern(final Configuration conf) {
        return conf.get(INPUT_SPLIT_KEY_PATTERN, "{ \"_id\": 1 }");
    }

    public static DBObject getInputSplitKey(final Configuration conf) {
        try {
            final String json = getInputSplitKeyPattern(conf);
            final DBObject obj = (DBObject) JSON.parse(json);
            if (obj == null) {
                return new BasicDBObject("_id", 1);
            } else {
                return obj;
            }
        } catch (final Exception e) {
            throw new IllegalArgumentException("Provided JSON String is not representable/parsable as a DBObject.", e);
        }
    }


    public static void setInputKey(final Configuration conf, final String fieldName) {
        // TODO (bwm) - validate key rules?
        conf.set(INPUT_KEY, fieldName);
    }

    public static String getInputKey(final Configuration conf) {
        return conf.get(INPUT_KEY, "_id");
    }

    public static void setNoTimeout(final Configuration conf, final boolean value) {
        conf.setBoolean(INPUT_NOTIMEOUT, value);
    }

    public static boolean isNoTimeout(final Configuration conf) {
        return conf.getBoolean(INPUT_NOTIMEOUT, false);
    }

    //BSON-specific config functions.
    public static boolean getBSONReadSplits(final Configuration conf) {
        return conf.getBoolean(BSON_READ_SPLITS, true);
    }

    public static void setBSONReadSplits(final Configuration conf, final boolean val) {
        conf.setBoolean(BSON_READ_SPLITS, val);
    }

    public static boolean getBSONWriteSplits(final Configuration conf) {
        return conf.getBoolean(BSON_WRITE_SPLITS, true);
    }

    public static void setBSONWriteSplits(final Configuration conf, final boolean val) {
        conf.setBoolean(BSON_WRITE_SPLITS, val);
    }

    public static boolean getBSONOutputBuildSplits(final Configuration conf) {
        return conf.getBoolean(BSON_OUTPUT_BUILDSPLITS, false);
    }

    public static void setBSONOutputBuildSplits(final Configuration conf, final boolean val) {
        conf.setBoolean(BSON_OUTPUT_BUILDSPLITS, val);
    }

    public static void setBSONPathFilter(final Configuration conf, final Class<? extends PathFilter> val) {
        conf.setClass(BSON_PATHFILTER, val, PathFilter.class);
    }

    public static Class<?> getBSONPathFilter(final Configuration conf) {
        return conf.getClass(BSON_PATHFILTER, null);
    }

    public static String getBSONSplitsPath(final Configuration conf) {
        return conf.get(BSON_SPLITS_PATH);
    }

    public static void setBSONSplitsPath(final Configuration conf, final
                                         String path) {
        conf.set(BSON_SPLITS_PATH, path);
    }

    public static Class<? extends MongoSplitter> getSplitterClass(final Configuration conf) {
        return conf.getClass(MONGO_SPLITTER_CLASS, null, MongoSplitter.class);
    }

    public static void setSplitterClass(final Configuration conf, final Class<? extends MongoSplitter> val) {
        conf.setClass(MONGO_SPLITTER_CLASS, val, MongoSplitter.class);
    }

    public static List<String> getInputMongosHosts(final Configuration conf) {
        String raw = conf.get(INPUT_MONGOS_HOSTS, null);

        if (raw == null || raw.length() == 0) {
            return Collections.emptyList(); // empty list - no mongos specified
        }

        // List of hostnames delimited by whitespace
        return Arrays.asList(StringUtils.split(raw));
    }

    public static void setInputMongosHosts(final Configuration conf, final List<String> hostnames) {
        String raw = "";
        if (hostnames != null) {
            raw = StringUtils.join(hostnames, ' ');
        }

        conf.set(INPUT_MONGOS_HOSTS, raw);
    }

    /**
     * Fetch a class by its actual class name, rather than by a key name in the configuration properties. We still need to pass in a
     * Configuration object here, since the Configuration class maintains an internal cache of class names for performance on some hadoop
     * versions. It also ensures that the same classloader is used across all keys.
     */
    public static <U> Class<? extends U> getClassByName(final Configuration conf,
                                                        final String className,
                                                        final Class<U> xface) {

        if (className == null) {
            return null;
        }
        try {
            Class<?> theClass = conf.getClassByName(className);
            if (theClass != null && !xface.isAssignableFrom(theClass)) {
                throw new RuntimeException(theClass + " not " + xface.getName());
            } else if (theClass != null) {
                return theClass.asSubclass(xface);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Configuration buildConfiguration(final Map<String, Object> data) {
        Configuration newConf = new Configuration();
        for (Entry<String, Object> entry : data.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (val instanceof String) {
                newConf.set(key, (String) val);
            } else if (val instanceof Boolean) {
                newConf.setBoolean(key, (Boolean) val);
            } else if (val instanceof Integer) {
                newConf.setInt(key, (Integer) val);
            } else if (val instanceof Float) {
                newConf.setFloat(key, (Float) val);
            } else if (val instanceof DBObject) {
                setDBObject(newConf, key, (DBObject) val);
            } else {
                throw new RuntimeException("can't convert " + val.getClass() + " into any type for Configuration");
            }
        }
        return newConf;
    }

    public static void close(final Mongo client) {
        synchronized (CLIENTS) {
            MongoClientURI uri = uriMap.remove(client);
            if (uri != null) {
                MongoClient remove;
                remove = CLIENTS.remove(uri);
                if (remove != client) {
                    throw new IllegalStateException("different clients found");
                }
            }
            client.close();
        }
    }
    
    private static MongoClient getMongoClient(final MongoClientURI uri) throws UnknownHostException {
        synchronized (CLIENTS) {
            MongoClient mongoClient = CLIENTS.get(uri);
            if (mongoClient == null) {
                mongoClient = new MongoClient(uri);
                CLIENTS.put(uri, mongoClient);
                uriMap.put(mongoClient, uri);
            }
            return mongoClient;
        }
    }
}
