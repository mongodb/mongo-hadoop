/*
 * Copyright 2011-2013 10gen Inc.
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

package com.mongodb.hadoop.output;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoOutput;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MongoRecordWriter<K, V> extends RecordWriter<K, V> {

    private static final Log LOG = LogFactory.getLog(MongoRecordWriter.class);

    private final List<DBCollection> collections;
    private final int numberOfHosts;
    private final TaskAttemptContext context;
    private final String[] updateKeys;
    private final boolean multiUpdate;

    private int roundRobinCounter = 0;

    public MongoRecordWriter(final DBCollection c, final TaskAttemptContext ctx) {
        this(c, ctx, null);
    }

    public MongoRecordWriter(final DBCollection c, final TaskAttemptContext ctx, final String[] updateKeys) {
        this(c, ctx, updateKeys, false);
    }

    public MongoRecordWriter(final DBCollection c, final TaskAttemptContext ctx, final String[] updateKeys, final boolean multi) {
        this(Arrays.asList(c), ctx, updateKeys, multi);
    }

    public MongoRecordWriter(final List<DBCollection> c, final TaskAttemptContext ctx) {
        this(c, ctx, null);
    }

    public MongoRecordWriter(final List<DBCollection> c, final TaskAttemptContext ctx, final String[] updateKeys) {
        this(c, ctx, updateKeys, false);
    }

    public MongoRecordWriter(final List<DBCollection> c, final TaskAttemptContext ctx, final String[] updateKeys, final boolean multi) {
        collections = new ArrayList<DBCollection>(c);
        context = ctx;
        this.updateKeys = updateKeys;
        this.multiUpdate = multi;
        this.numberOfHosts = c.size();

        //authenticate if necessary - but don't auth twice on same DB
        MongoConfig config = new MongoConfig(ctx.getConfiguration());
        if (config.getAuthURI() != null) {
            MongoURI authURI = config.getAuthURI();
            if (authURI.getUsername() != null && authURI.getPassword() != null) {
                // need to verify that it is not already one of the collections we are writing to.
                DBCollection collection = collections.get(0);
                DB targetAuthDB = collection.getDB().getSisterDB(authURI.getDatabase());
                if (!targetAuthDB.isAuthenticated()) {
                    targetAuthDB.authenticate(authURI.getUsername(), authURI.getPassword());
                }
            }
        }
    }

    public void close(final TaskAttemptContext context) {
        for (DBCollection collection : collections) {
            collection.getDB().getLastError();
        }
    }

    public void write(final K key, final V value) throws IOException {
        final DBObject o = new BasicDBObject();

        if (value instanceof MongoUpdateWritable) {
            //ignore the key - just use the update directly.
            MongoUpdateWritable muw = (MongoUpdateWritable) value;
            try {
                DBCollection dbCollection = getDbCollectionByRoundRobin();
                dbCollection.update(new BasicDBObject(muw.getQuery()),
                                    new BasicDBObject(muw.getModifiers()),
                                    muw.isUpsert(),
                                    muw.isMultiUpdate());
                return;
            } catch (final MongoException e) {
                throw new IOException("can't write to mongo", e);
            }
        }

        if (key instanceof BSONWritable) {
            o.put("_id", ((BSONWritable) key).getDoc());
        } else if (key instanceof BSONObject) {
            o.put("_id", key);
        } else {
            o.put("_id", BSONWritable.toBSON(key));
        }

        if (value instanceof BSONWritable) {
            o.putAll(((BSONWritable) value).getDoc());
        } else if (value instanceof MongoOutput) {
            ((MongoOutput) value).appendAsValue(o);
        } else if (value instanceof BSONObject) {
            o.putAll((BSONObject) value);
        } else {
            o.put("value", BSONWritable.toBSON(value));
        }

        try {
            DBCollection dbCollection = getDbCollectionByRoundRobin();

            if (updateKeys == null) {
                dbCollection.save(o);
            } else {
                // Form the query fields
                DBObject query = new BasicDBObject(updateKeys.length);
                for (String updateKey : updateKeys) {
                    query.put(updateKey, o.get(updateKey));
                    o.removeField(updateKey);
                }
                // If _id is null remove it, we don't want to override with null _id
                if (o.get("_id") == null) {
                    o.removeField("_id");
                }
                DBObject set = new BasicDBObject().append("$set", o);
                dbCollection.update(query, set, true, multiUpdate);
            }
        } catch (final MongoException e) {
            throw new IOException("can't write to mongo", e);
        }
    }

    private synchronized DBCollection getDbCollectionByRoundRobin() {
        int hostIndex = (roundRobinCounter++ & 0x7FFFFFFF) % numberOfHosts;
        return collections.get(hostIndex);
    }

    public void ensureIndex(final DBObject index, final DBObject options) {
        // just do it on one mongod
        DBCollection dbCollection = collections.get(0);
        dbCollection.ensureIndex(index, options);
    }

    public TaskAttemptContext getContext() {
        return context;
    }
}

