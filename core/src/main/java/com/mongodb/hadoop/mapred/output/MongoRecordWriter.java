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
import java.util.List;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import org.bson.*;

import com.mongodb.*;
import com.mongodb.hadoop.*;

public class MongoRecordWriter<K, V> implements RecordWriter<K, V> {

    public MongoRecordWriter(List<DBCollection> c, JobConf conf) {
        _collections = c;
        _conf = conf;
        _numberOfHosts = c.size();
    }

    public void close(Reporter reporter) {
        for (DBCollection collection : _collections) {
            collection.getDB().getLastError();
        }
    }



    public void write(K key, V value) throws IOException {
         final DBObject o = new BasicDBObject();

        if ( key instanceof BSONWritable ){
            o.put("_id", ((BSONWritable)key).getDoc());
        }
        else if ( key instanceof BSONObject ){
            o.put( "_id", key );
        }
        else{
            o.put( "_id", BSONWritable.toBSON(key) );
        }

        if (value instanceof BSONWritable ){
            o.putAll( ((BSONWritable)value).getDoc() );
        }
        else if ( value instanceof MongoOutput ){
            ( (MongoOutput) value ).appendAsValue( o );
        }
        else if ( value instanceof BSONObject ){
            o.putAll( (BSONObject) value );
        }
        else{
            o.put( "value", BSONWritable.toBSON( value ) );
        }

        try {
            DBCollection dbCollection = getDbCollectionByRoundRobin();
            dbCollection.save(o);
        } catch ( final MongoException e ) {
            e.printStackTrace();
            throw new IOException( "can't write to mongo", e );
        } 

    }

    private synchronized DBCollection getDbCollectionByRoundRobin() {
        int hostIndex = (_roundRobinCounter++ & 0x7FFFFFFF) % _numberOfHosts;
        return _collections.get(hostIndex);
    }

    public JobConf getConf() {
        return _conf;
    }

    int _roundRobinCounter = 0;
    final int _numberOfHosts;
    final List<DBCollection> _collections;
    final JobConf _conf;

    private static final Log log = LogFactory.getLog(MongoRecordWriter.class);

}
