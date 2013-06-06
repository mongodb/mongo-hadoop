// MongoRecordReader.java
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

package com.mongodb.hadoop.mapred.input;

import java.io.IOException;
import com.mongodb.*;
import com.mongodb.hadoop.io.*;
import com.mongodb.hadoop.input.MongoInputSplit;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import org.bson.*;


@SuppressWarnings( "deprecation" )
public class MongoRecordReader implements RecordReader<BSONWritable, BSONWritable> {

    public MongoRecordReader( MongoInputSplit split ){
        _split = split;
        _cursor = split.getCursor();
        _keyField = split.getKeyField();
    }

    public void close(){
        if ( _cursor != null )
            _cursor.close();
    }

    public BSONWritable createKey(){
        return new BSONWritable();
    }


    public BSONWritable createValue(){
        return new BSONWritable();
    }

    public BSONWritable getCurrentKey(){
        return this.currentKey;
    }

    public BSONWritable getCurrentValue(){
        return this.currentVal;
    }

    public float getProgress(){
        try {
            if ( _cursor.hasNext() ){
                return 0.0f;
            }else{
                return 1.0f;
            }
        } catch ( MongoException e ) {
            return 1.0f;
        }
    }

    public long getPos(){
        return 0; // no progress to be reported, just working on it
    }

    public void initialize( InputSplit split, TaskAttemptContext context ){
        _total = 1.0f;
    }

    public boolean nextKeyValue() throws IOException{
        try {
            if ( !_cursor.hasNext() ){
                log.info("Read " + _seen + " documents from:");
                log.info(_split.toString());
                return false;
            }

            DBObject next = _cursor.next();
            this.currentVal.setDoc(next);
            this.currentKey.setDoc(new BasicBSONObject("_id", next.get("_id")));
            _seen++;

            return true;
        } catch ( MongoException e ) {
            throw new IOException("Couldn't get next key/value from mongodb: ", e);
        }
    }

     public boolean next( BSONWritable key, BSONWritable value ) throws IOException{
         if ( nextKeyValue() ){
             key.setDoc(this.currentKey.getDoc());
             value.setDoc(this.currentVal.getDoc());
             return true;
         } else{
             log.info( "Cursor exhausted." );
             return false;
         }
     }

    private final DBCursor _cursor;
    private BSONWritable currentVal = new BSONWritable();
    private BSONWritable currentKey = new BSONWritable();
    private BasicDBObject _current;
    private float _seen = 0;
    private float _total;
    private String _keyField;
    private MongoInputSplit _split;

    private static final Log log = LogFactory.getLog( MongoRecordReader.class );
}
