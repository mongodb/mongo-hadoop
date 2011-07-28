// MongoRecordWriter.java
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

package com.mongodb.hadoop.mapred.output;

import java.io.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.bson.*;

import com.mongodb.*;
import com.mongodb.hadoop.*;

public class MongoRecordWriter<K, V> implements RecordWriter<K, V> {

    public MongoRecordWriter(DBCollection c, JobConf conf) {
        _collection = c;
        _conf = conf;
    }

    public void close(Reporter reporter) {
        _collection.getDB().getLastError();
    }

    Object toBSON(Object x) {
        if (x == null) return null;
        if (x instanceof Text || x instanceof UTF8) return x.toString();
        if (x instanceof Writable) {
            if (x instanceof AbstractMapWritable)
                throw new IllegalArgumentException("ERROR: MapWritables are not presently supported for MongoDB Serialization.");
            if (x instanceof ArrayWritable) { // TODO - test me
                Writable[] o = ((ArrayWritable) x).get();
                Object[] a = new Object[o.length];
                for (int i = 0; i < o.length; i++)
                    a[i] = (Writable) toBSON(o[i]);
            }
            if (x instanceof BooleanWritable) return ((BooleanWritable) x).get();
            if (x instanceof BytesWritable) return ((BytesWritable) x).getBytes();
            if (x instanceof ByteWritable) return ((ByteWritable) x).get();
            if (x instanceof DoubleWritable) return ((DoubleWritable) x).get();
            if (x instanceof FloatWritable) return ((FloatWritable) x).get();
            if (x instanceof LongWritable) return ((LongWritable) x).get();
            if (x instanceof IntWritable) return ((IntWritable) x).get();

            // TODO - Support counters

        }
        throw new RuntimeException("can't convert: " + x.getClass().getName() + " to BSON");
    }

    public void write(K key, V value) throws IOException {
        final DBObject o = new BasicDBObject();

        log.trace( "Writing out data {k: " + key + ", value:  " + value);
        if (key instanceof MongoOutput) {
            ((MongoOutput) key).appendAsKey(o);
        }
        else if (key instanceof BSONObject) {
            o.put("_id", key);
        }
        else {
            o.put("_id", toBSON(key));
        }

        if (value instanceof MongoOutput) {
            ((MongoOutput) value).appendAsValue(o);
        }
        else if (value instanceof BSONObject) {
            o.putAll((BSONObject) value);
        }
        else {
            o.put("value", toBSON(value));
        }

        try {
            _collection.save(o);
        }
        catch (final MongoException e) {
            throw new IOException("can't write to mongo", e);
        }
    }

    public JobConf getConf() {
        return _conf;
    }

    final DBCollection _collection;
    final JobConf _conf;

    private static final Log log = LogFactory.getLog(MongoRecordWriter.class);

}
