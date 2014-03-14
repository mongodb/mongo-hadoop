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


package com.mongodb.hadoop.mapred.output;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.hadoop.MongoOutput;
import com.mongodb.hadoop.io.BSONWritable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.List;

public class MongoRecordWriter<K, V> implements RecordWriter<K, V> {

    private int roundRobinCounter = 0;
    private final int numberOfHosts;
    private final List<DBCollection> collections;

    private final JobConf configuration;
    
    private final String[] updateKeys;
    private final boolean multiUpdate;
    
    public MongoRecordWriter(final List<DBCollection> c, final JobConf conf) {
        this(c, conf, null, false);
    }


    public MongoRecordWriter(final List<DBCollection> c, final JobConf conf, String[] updateKeys, boolean multiUpdate) {
        collections = c;
        configuration = conf;
        numberOfHosts = c.size();
        this.updateKeys = updateKeys;
        this.multiUpdate = multiUpdate;
    }


    public void close(final Reporter reporter) {
        for (DBCollection collection : collections) {
            collection.getDB().getLastError();
        }
    }

    public void write(final K key, final V value) throws IOException {
        final DBObject o = new BasicDBObject();

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

    public JobConf getConf() {
        return configuration;
    }

}
