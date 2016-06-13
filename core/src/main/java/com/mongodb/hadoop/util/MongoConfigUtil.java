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
import com.mongodb.hadoop.splitter.SampleSplitter;
import com.mongodb.util.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

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
    public static final String OUTPUT_BATCH_SIZE = "mongo.output.batch.size";
    public static final String OUTPUT_BULK_ORDERED = "mongo.output.bulk.ordered";

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

    // Settings specific to reading from GridFS.
    public static final String GRIDFS_DELIMITER_PATTERN =
      "mongo.gridfs.delimiter.pattern";
    public static final String GRIDFS_DEFAULT_DELIMITER = "(\n|\r\n)";
    public static final String GRIDFS_WHOLE_FILE_SPLIT =
      "mongo.gridfs.whole_file";
    public static final String GRIDFS_READ_BINARY =
      "mongo.gridfs.read_binary";

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
     * When {@code true}, MongoSplitter implementations will check for and
     * remove empty splits before returning them from {@code calculateSplits}.
     * This requires pulling a small amount of data from MongoDB but avoids
     * starting tasks that don't have any data to process.
     *
     * This option is useful when providing a query to {@link #INPUT_QUERY},
     * and that query selects a subset of the documents in the collection
     * that are clustered in the same range of the index described by
     * {@link #INPUT_SPLIT_KEY_PATTERN}. If the query selects documents
     * throughout the index, consider using
     * {@link com.mongodb.hadoop.splitter.MongoPaginatingSplitter} and setting
     * {@link #INPUT_SPLIT_MIN_DOCS} instead.
     */
    public static final String ENABLE_FILTER_EMPTY_SPLITS =
      "mongo.input.splits.filter_empty";

    /**
     * When {@link #SPLITS_USE_RANGEQUERY} is enabled, this option sets the
     * minimum number of documents to be contained in each MongoInputSplit
     * (does not apply to BSON). This option only applies when using
     * {@link com.mongodb.hadoop.splitter.MongoPaginatingSplitter} as the
     * splitter implementation.
     *
     * This value defaults to {@link #DEFAULT_INPUT_SPLIT_MIN_DOCS}.
     */
    public static final String INPUT_SPLIT_MIN_DOCS =
      "mongo.input.splits.min_docs";
    public static final int DEFAULT_INPUT_SPLIT_MIN_DOCS = 1000;

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
     * To customize the range of the index that is used to create splits, see
     * the {@link #INPUT_SPLIT_KEY_MIN} and {@link #INPUT_SPLIT_KEY_MAX}
     * settings.
     * </p>
     * <p>
     * This must be a JSON document, and not just a field name!
     * </p>
     *
     * @see <a href="http://docs.mongodb.org/manual/core/sharding-shard-key/">Shard Keys</a>
     */
    public static final String INPUT_SPLIT_KEY_PATTERN = "mongo.input.split.split_key_pattern";

    /**
     * Lower-bound for splits created using the index described by
     * {@link #INPUT_SPLIT_KEY_PATTERN}. This value must be set to a JSON
     * string that describes a point in the index. This setting must be used
     * in conjunction with {@code INPUT_SPLIT_KEY_PATTERN} and
     * {@link #INPUT_SPLIT_KEY_MAX}.
     */
    public static final String INPUT_SPLIT_KEY_MIN = "mongo.input.split.split_key_min";

    /**
     * Upper-bound for splits created using the index described by
     * {@link #INPUT_SPLIT_KEY_PATTERN}. This value must be set to a JSON
     * string that describes a point in the index. This setting must be used
     * in conjuntion with {@code INPUT_SPLIT_KEY_PATTERN} and
     * {@link #INPUT_SPLIT_KEY_MIN}.
     */
    public static final String INPUT_SPLIT_KEY_MAX = "mongo.input.split.split_key_max";

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
     * One client per thread
     */
    private static final ThreadLocal<Map<MongoClientURI, MongoClient>> CLIENTS =
      new ThreadLocal<Map<MongoClientURI, MongoClient>>() {
          @Override public Map<MongoClientURI, MongoClient> initialValue() {
              return new HashMap<MongoClientURI, MongoClient>();
          }
      };

    private static final ThreadLocal<Map<MongoClient, MongoClientURI>> URI_MAP =
      new ThreadLocal<Map<MongoClient, MongoClientURI>>() {
          @Override
          public Map<MongoClient, MongoClientURI> initialValue() {
              return new HashMap<MongoClient, MongoClientURI>();
          }
      };

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
        String raw = conf.get(key);
        List<MongoClientURI> result = new LinkedList<MongoClientURI>();
        if (raw != null && !raw.trim().isEmpty()) {
            for (String connectionString : raw.split("mongodb://")) {
                // Try to be forgiving with formatting.
                connectionString = StringUtils.strip(connectionString, ", ");
                if (!connectionString.isEmpty()) {
                    result.add(
                      new MongoClientURI("mongodb://" + connectionString));
                }
            }
        }
        return result;
    }

    /**
     * @deprecated use {@link #getMongoClientURI(Configuration, String)} instead
     * @param conf the Configuration
     * @param key the key for the setting
     * @return the MongoURI stored for the given key
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

    /**
     * Retrieve a setting as a {@code MongoClientURI}.
     * @param conf the Configuration
     * @param key the key for the setting
     * @return the MongoClientURI stored for the given key
     */
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
     * @param uri the MongoDB URI
     * @return the DBCollection in the URI
     */
    @Deprecated
    public static DBCollection getCollection(final MongoURI uri) {
        return getCollection(new MongoClientURI(uri.toString()));
    }

    /**
     * Retrieve a DBCollection from a MongoDB URI.
     * @param uri the MongoDB URI
     * @return the DBCollection in the URI
     */
    public static DBCollection getCollection(final MongoClientURI uri) {
        try {
            return getMongoClient(uri).getDB(uri.getDatabase()).getCollection(uri.getCollection());
        } catch (Exception e) {
            throw new IllegalArgumentException("Couldn't connect and authenticate to get collection", e);
        }
    }

    /**
     * @deprecated use {@link #getCollectionWithAuth(MongoClientURI, MongoClientURI)} instead
     * @param authURI the URI with which to authenticate
     * @param uri the MongoDB URI
     * @return the authenticated DBCollection
     */
    @Deprecated
    public static DBCollection getCollectionWithAuth(final MongoURI uri, final MongoURI authURI) {
        return getCollectionWithAuth(
          new MongoClientURI(uri.toString()),
          new MongoClientURI(authURI.toString()));
    }

    /**
     * Get an authenticated DBCollection from a MongodB URI.
     * @param authURI the URI with which to authenticate
     * @param uri the MongoDB URI
     * @return the authenticated DBCollection
     */
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
     * @param conf the Configuration
     * @param key the key for the setting
     * @param value the value for the setting
     */
    @Deprecated
    public static void setMongoURI(final Configuration conf, final String key, final MongoURI value) {
        conf.set(key, value.toString()); // todo - verify you can toString a
        // URI object
    }

    /**
     * Helper for providing a {@code MongoClientURI} as the value for a setting.
     * @param conf  the Configuration
     * @param key   the key for the setting
     * @param value the value for the setting
     */
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
     * @param conf the Configuration
     * @param uri the MongoURI
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static void setInputURI(final Configuration conf, final MongoURI uri) {
        setMongoURI(conf, INPUT_URI, uri);
    }

    /**
     * Set the input URI for the job.
     * @param conf the Configuration
     * @param uri the MongoDB URI
     */
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
     * @param conf the Configuration
     * @param uri the MongoDB URI
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static void setOutputURI(final Configuration conf, final MongoURI uri) {
        setMongoURI(conf, OUTPUT_URI, uri);
    }

    /**
     * Set the output URI for the job.
     * @param conf the Configuration
     * @param uri the MongoDB URI
     */
    public static void setOutputURI(final Configuration conf, final MongoClientURI uri) {
        setMongoURI(conf, OUTPUT_URI, uri);
    }

    /**
     * Get the maximum number of documents that should be loaded into memory
     * and sent in a batch to MongoDB as the output of a job.
     * @param conf the Configuration
     * @return the number of documents
     */
    public static int getBatchSize(final Configuration conf) {
        return conf.getInt(OUTPUT_BATCH_SIZE, 1000);
    }

    /**
     * Get whether or not bulk writes will be ordered
     * and sent in a batch to MongoDB as the output of a job.
     * @param conf the Configuration
     * @return true if bulk writes will be ordered, false otherwise
     */
    public static boolean isBulkOrdered(final Configuration conf) {
        return conf.getBoolean(OUTPUT_BULK_ORDERED, true);
    }

    /**
     * Set whether or not bulk writes should be ordered.
     * @param conf the Configuration
     * @param ordered true if bulk writes should be ordered, false otherwise
     */
    public static void setBulkOrdered(final Configuration conf, final boolean ordered) {
        conf.setBoolean(OUTPUT_BULK_ORDERED, ordered);
    }

    /**
     * Set the maximum number of documents that should be loaded into memory
     * and sent in a batch to MongoDB as the output of a job.
     * @param conf the Configuration
     * @param size the number of documents
     */
    public static void setBatchSize(final Configuration conf, final int size) {
        conf.setInt(OUTPUT_BATCH_SIZE, size);
    }

    /**
     * Helper for providing a JSON string as a value for a setting.
     * @param conf the Configuration
     * @param key the key for the setting
     * @param value the JSON string value
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

    /**
     * Set the query set for the Job using a DBObject.
     * @param conf the Configuration
     * @param query the query
     */
    public static void setQuery(final Configuration conf, final DBObject query) {
        setDBObject(conf, INPUT_QUERY, query);
    }

    public static DBObject getQuery(final Configuration conf) {
        return getDBObject(conf, INPUT_QUERY);
    }

    public static void setFields(final Configuration conf, final String fields) {
        setJSON(conf, INPUT_FIELDS, fields);
    }

    /**
     * Specify a projection document for documents retrieved from MongoDB.
     * @param conf the Configuration
     * @param fields a projection document
     */
    public static void setFields(final Configuration conf, final DBObject fields) {
        setDBObject(conf, INPUT_FIELDS, fields);
    }

    public static DBObject getFields(final Configuration conf) {
        return getDBObject(conf, INPUT_FIELDS);
    }

    public static void setSort(final Configuration conf, final String sort) {
        setJSON(conf, INPUT_SORT, sort);
    }

    /**
     * Specify the sort order as a DBObject.
     * @param conf the Configuration
     * @param sort the sort document
     */
    public static void setSort(final Configuration conf, final DBObject sort) {
        setDBObject(conf, INPUT_SORT, sort);
    }

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

    public static void setEnableFilterEmptySplits(
      final Configuration conf, final boolean value) {
        conf.setBoolean(ENABLE_FILTER_EMPTY_SPLITS, value);
    }

    public static boolean isFilterEmptySplitsEnabled(final Configuration conf) {
        return conf.getBoolean(ENABLE_FILTER_EMPTY_SPLITS, false);
    }

    public static void setInputSplitMinDocs(
      final Configuration conf, final int value) {
        if (value < 0) {
            throw new IllegalArgumentException(
              INPUT_SPLIT_MIN_DOCS + " must be greater than 0.");
        }
        conf.setInt(INPUT_SPLIT_MIN_DOCS, value);
    }

    public static int getInputSplitMinDocs(final Configuration conf) {
        return conf.getInt(INPUT_SPLIT_MIN_DOCS, DEFAULT_INPUT_SPLIT_MIN_DOCS);
    }

    public static boolean isRangeQueryEnabled(final Configuration conf) {
        return conf.getBoolean(SPLITS_USE_RANGEQUERY, false);
    }

    /**
     * Enable using {@code $lt} and {@code $gt} to define InputSplits rather
     * than {@code $min} and {@code $max}. This allows the database's query
     * optimizer to choose the best index instead of using the one in the
     * $max/$min keys. This will only work if the key used for splitting is
     * *not* a compound key. Make sure that all values under the splitting key
     * are of the same type, or this will cause incomplete results.
     *
     * @param conf the Configuration
     * @param value enables using {@code $lt} and {@code $gt}
     */
    public static void setRangeQueryEnabled(final Configuration conf, final boolean value) {
        conf.setBoolean(SPLITS_USE_RANGEQUERY, value);
    }

    public static boolean canReadSplitsFromShards(final Configuration conf) {
        return conf.getBoolean(SPLITS_USE_SHARDS, false);
    }

    /**
     * Set whether the reading directly from shards is enabled.
     *
     * When {@code true}, splits are read directly from shards. By default,
     * splits are read through a mongos router when connected to a sharded
     * cluster. Note that reading directly from shards can lead to bizarre
     * results when there are orphaned documents or if the balancer is running.
     *
     * @param conf the Configuration
     * @param value enables reading from shards directly
     *
     * @see <a href="http://docs.mongodb.org/manual/core/sharding-balancing/">Sharding Balancing</a>
     * @see <a href="http://docs.mongodb.org/manual/reference/command/cleanupOrphaned/#dbcmd.cleanupOrphaned">cleanupOrphaned command</a>
     */
    public static void setReadSplitsFromShards(final Configuration conf, final boolean value) {
        conf.setBoolean(SPLITS_USE_SHARDS, value);
    }

    public static boolean isShardChunkedSplittingEnabled(final Configuration conf) {
        return conf.getBoolean(SPLITS_USE_CHUNKS, true);
    }

    public static int getSamplesPerSplit(final Configuration conf) {
        return conf.getInt(
          SampleSplitter.SAMPLES_PER_SPLIT,
          SampleSplitter.DEFAULT_SAMPLES_PER_SPLIT);
    }

    public static void setSamplesPerSplit(
      final Configuration conf, final int samples) {
        conf.setInt(SampleSplitter.SAMPLES_PER_SPLIT, samples);
    }

    /**
     * Set whether using shard chunk splits as InputSplits is enabled.
     * @param conf the Configuration
     * @param value enables using shard chunk splits as InputSplits.
     */
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

    public static DBObject getMinSplitKey(final Configuration configuration) {
        return getDBObject(configuration, INPUT_SPLIT_KEY_MIN);
    }

    public static DBObject getMaxSplitKey(final Configuration configuration) {
        return getDBObject(configuration, INPUT_SPLIT_KEY_MAX);
    }

    public static void setMinSplitKey(
      final Configuration conf, final String string) {
        conf.set(INPUT_SPLIT_KEY_MIN, string);
    }

    public static void setMaxSplitKey(
      final Configuration conf, final String string) {
        conf.set(INPUT_SPLIT_KEY_MAX, string);
    }

    public static void setInputKey(final Configuration conf, final String fieldName) {
        // TODO (bwm) - validate key rules?
        conf.set(INPUT_KEY, fieldName);
    }

    public static String getInputKey(final Configuration conf) {
        return conf.get(INPUT_KEY, "_id");
    }

    public static String getGridFSDelimiterPattern(final Configuration conf) {
        return conf.get(GRIDFS_DELIMITER_PATTERN, GRIDFS_DEFAULT_DELIMITER);
    }

    public static void setGridFSDelimiterPattern(
      final Configuration conf, final String pattern) {
        conf.set(GRIDFS_DELIMITER_PATTERN, pattern);
    }

    public static boolean isGridFSWholeFileSplit(final Configuration conf) {
        return conf.getBoolean(GRIDFS_WHOLE_FILE_SPLIT, false);
    }

    public static void setGridFSWholeFileSplit(
      final Configuration conf, final boolean split) {
        conf.setBoolean(GRIDFS_WHOLE_FILE_SPLIT, split);
    }

    public static boolean isGridFSReadBinary(final Configuration conf) {
        return conf.getBoolean(GRIDFS_READ_BINARY, false);
    }

    public static void setGridFSReadBinary(
      final Configuration conf, final boolean readBinary) {
        conf.setBoolean(GRIDFS_READ_BINARY, readBinary);
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
     * Fetch a class by its name, rather than by a key name in the
     * Configuration properties. The Configuration class is used for its
     * internal cache of class names and to ensure that the same ClassLoader is
     * used across all keys.
     * @param conf the Configuration
     * @param className the name of the class
     * @param xface an interface or superclass of expected class
     * @param <U> the type of xface
     * @return the class or {@code null} if not found
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
            MongoClientURI uri = URI_MAP.get().remove(client);
            if (uri != null) {
                MongoClient remove;
                remove = CLIENTS.get().remove(uri);
                if (remove != client) {
                    throw new IllegalStateException("different clients found");
                }
                client.close();
            }
    }

    private static MongoClient getMongoClient(final MongoClientURI uri) throws UnknownHostException {
        MongoClient mongoClient = CLIENTS.get().get(uri);
            if (mongoClient == null) {
                mongoClient = new MongoClient(uri);
                CLIENTS.get().put(uri, mongoClient);
                URI_MAP.get().put(mongoClient, uri);
            }
            return mongoClient;
        }

    /**
     * Load Properties stored in a .properties file.
     * @param conf the Configuration
     * @param filename the path to the properties file
     * @return the Properties in the file
     * @throws IOException if there was a problem reading the properties file
     */
    public static Properties readPropertiesFromFile(
      final Configuration conf, final String filename)
      throws IOException {
        Path propertiesFilePath = new Path(filename);
        FileSystem fs = FileSystem.get(URI.create(filename), conf);
        if (!fs.exists(propertiesFilePath)) {
            throw new IOException(
              "Properties file does not exist: " + filename);
        }
        FSDataInputStream inputStream = fs.open(propertiesFilePath);
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } finally {
            inputStream.close();
        }
        return properties;
    }
}
