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
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.hadoop.MongoOutput;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MongoRecordWriter<K, V> extends RecordWriter<K, V> {

    private final List<DBCollection> collections;
    private final int numberOfHosts;
    private final TaskAttemptContext context;

    private int roundRobinCounter = 0;

    public MongoRecordWriter(DBCollection c, TaskAttemptContext ctx) {
        this(Arrays.asList(c), ctx);
    }

    public MongoRecordWriter(List<DBCollection> c, TaskAttemptContext ctx) {
        collections = new ArrayList<DBCollection>(c);
        context = ctx;
        this.numberOfHosts = c.size();
    }

    public void close(final TaskAttemptContext context) {
    }

    public void write(final K key, final V value) throws IOException {
        final DBObject o = new BasicDBObject();

        if (value instanceof MongoUpdateWritable) {
            //ignore the key - just use the update directly.
            MongoUpdateWritable muw = (MongoUpdateWritable) value;
            try {
                DBCollection dbCollection = getDbCollectionByRoundRobin();
                dbCollection.update(new BasicDBObject(muw.getQuery()), new BasicDBObject(muw.getModifiers()), muw.isUpsert(),
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
            dbCollection.save(o);
        } catch (final MongoException e) {
            e.printStackTrace();
            throw new IOException("can't write to mongo", e);
        }
    }

    private synchronized DBCollection getDbCollectionByRoundRobin() {
        int hostIndex = (roundRobinCounter++ & 0x7FFFFFFF) % numberOfHosts;
        return collections.get(hostIndex);
    }

    public void ensureIndex(final DBObject index, final DBObject options) {
        // just do it on one mongod
        collections.get(0).createIndex(index, options);
    }

    public TaskAttemptContext getContext() {
        return context;
    }
}

