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

import com.mongodb.hadoop.io.BSONWritable;
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



    public void write(K key, V value) throws IOException {
        final DBObject o = new BasicDBObject();

        if (log.isTraceEnabled()) log.trace( "Writing out data {k: " + key + ", value:  " + value);
        if (key instanceof MongoOutput) {
            ((MongoOutput) key).appendAsKey(o);
        }
        else if (key instanceof BSONObject) {
            o.put("_id", key);
        }
        else {
            o.put("_id", BSONWritable.toBSON(key));
        }

        if (value instanceof MongoOutput) {
            ((MongoOutput) value).appendAsValue(o);
        }
        else if (value instanceof BSONObject) {
            o.putAll((BSONObject) value);
        }
        else {
            o.put("value", BSONWritable.toBSON(value));
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
