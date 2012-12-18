/*
 * Copyright 2011 10gen Inc.
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

import com.mongodb.*;
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MongoRecordWriter<K, V> extends RecordWriter<K, V> {
    
    private final String[] updateKeys;
    private final boolean multiUpdate;

    public MongoRecordWriter( DBCollection c, TaskAttemptContext ctx ){
        this(c, ctx, null);
    }
    
    public MongoRecordWriter( DBCollection c, TaskAttemptContext ctx, String[] updateKeys) {
        this(c, ctx, updateKeys, false);
    }
    
    public MongoRecordWriter( DBCollection c, TaskAttemptContext ctx, String[] updateKeys, boolean multi) {
        this(Arrays.asList(c), ctx, updateKeys, multi);
    }

    public MongoRecordWriter( List<DBCollection> c, TaskAttemptContext ctx ){
        this(c, ctx, null);
    }

    public MongoRecordWriter( List<DBCollection> c, TaskAttemptContext ctx, String[] updateKeys) {
        this(c, ctx, updateKeys, false);
    }

    public MongoRecordWriter( List<DBCollection> c, TaskAttemptContext ctx, String[] updateKeys, boolean multi) {
        _collections = new ArrayList<DBCollection>(c);
        _context = ctx;
        this.updateKeys = updateKeys;
        this.multiUpdate = false;
        this.numberOfHosts = c.size();
    }

    public void close( TaskAttemptContext context ){
        for (DBCollection collection : _collections) {
            collection.getDB().getLastError();
        }
    }

    public void write( K key, V value ) throws IOException{
        final DBObject o = new BasicDBObject();

        if ( key instanceof MongoOutput ){
            ( (MongoOutput) key ).appendAsKey( o );
        }
        else if ( key instanceof BSONObject ){
            o.put( "_id", key );
        }
        else{
            o.put( "_id", BSONWritable.toBSON(key) );
        }

        if ( value instanceof MongoOutput ){
            ( (MongoOutput) value ).appendAsValue( o );
        }
        else if ( value instanceof BSONObject ){
            o.putAll( (BSONObject) value );
        }
        else{
            o.put( "value", BSONWritable.toBSON( value ) );
        }

        try {
            // CARSTEN do the calculation
            DBCollection dbCollection = getDbCollectionByHashCode(key, value);

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
        }
        catch ( final MongoException e ) {
            throw new IOException( "can't write to mongo", e );
        }
    }

    private DBCollection getDbCollectionByHashCode(K key, V value) {
        int hash = key.hashCode() + value.hashCode();
        int hostIndex = (hash & 0x7FFFFFFF) % numberOfHosts;
        return _collections.get(hostIndex);
    }

    public void ensureIndex(DBObject index, DBObject options) {
        // just do it on one mongodS
        DBCollection dbCollection = _collections.get(0);
        dbCollection.ensureIndex(index, options);
    }

    public TaskAttemptContext getContext(){
        return _context;
    }

    final List<DBCollection> _collections;
    final int numberOfHosts;
    final TaskAttemptContext _context;
}

