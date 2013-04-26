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

package com.mongodb.hadoop.input;

// Mongo

import com.mongodb.DBCursor;
import com.mongodb.MongoException;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;

// Hadoop
// Commons

public class MongoRecordReader extends RecordReader<Object, BSONObject> {

    public MongoRecordReader( MongoInputSplit split ){
        _split = split;
        _cursor = split.getCursor();
    }

    @Override
    public void close(){
        if ( _cursor != null )
            _cursor.close();
    }

    @Override
    public Object getCurrentKey(){
        return _current.get( _split.getKeyField() );
    }

    @Override
    public BSONObject getCurrentValue(){
        return _current;
    }

    public float getProgress(){
        try {
            if ( _cursor.hasNext() ){
                return 0.0f;
            }
            else{
                return 1.0f;
            }
        }
        catch ( MongoException e ) {
            return 1.0f;
        }
    }

    @Override
    public void initialize( InputSplit split, TaskAttemptContext context ){
        _total = 1.0f;
    }

    @Override
    public boolean nextKeyValue(){
        try {
            if ( !_cursor.hasNext() )
                return false;

            _current = _cursor.next();
            _seen++;

            return true;
        }
        catch ( MongoException e ) {
            return false;
        }
    }


    private BSONObject _current;
    private final MongoInputSplit _split;
    private final DBCursor _cursor;
    private float _seen = 0;
    private float _total;

    private static final Log LOG = LogFactory.getLog( MongoRecordReader.class );

}
