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

package com.mongodb.hadoop.input;

import org.apache.commons.logging.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;

import com.mongodb.*;

public class MongoRecordReader extends RecordReader<Object, BSONObject> {
    private static final Log log = LogFactory.getLog( MongoRecordReader.class );

    public MongoRecordReader(MongoInputSplit split) {
        _split = split;
        _cursor = _split.getCursor();
    }

    public void close(){
    }

    public Object getCurrentKey(){
        return _cur.get( "_id" );
    }

    public BSONObject getCurrentValue(){
        return _cur;
    }

    public float getProgress(){
        return _seen / _total;
    }

    public void initialize( InputSplit split , TaskAttemptContext context ){
        if ( split != _split )
            throw new IllegalStateException( "split != _split ??? " );
        _total = _cursor.size();
    }

    public boolean nextKeyValue(){
        if ( !_cursor.hasNext() )
            return false;
        _cur = _cursor.next();
        _seen++;
        return true;
    }

    final MongoInputSplit _split;
    final DBCursor _cursor;

    BSONObject _cur;
    float _seen = 0;
    float _total;
}
