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
import org.bson.BSONObject;

// Hadoop
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

// Commons
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MongoRecordReader extends RecordReader<Object, BSONObject> {

    @Override
    public void close(){
        if ( _cursor != null )
            _cursor.close();
    }

    @Override
    public Object getCurrentKey() {
        return _current.get( "_id" );
    }

    @Override
    public BSONObject getCurrentValue() {
        return _current;
    }

    public float getProgress() throws MongoException {
    	try{
    		if (_cursor.hasNext()) {
    			return 0.0f;
    		} else {
    			return 1.0f;
    		}
    	}catch(MongoException ex){
    		return 1.0f;
    	}
    }

    @Override
    public void initialize( InputSplit split , TaskAttemptContext context ) {
        _total = _cursor.size();
        _total = 1.0f;
    }

    @Override
    public boolean nextKeyValue() throws MongoException{
    	try {
    		if ( !_cursor.hasNext() )
    			return false;
    	}catch(MongoException ex){
    		return false;
    	}

        _current = _cursor.next();
        _seen++;

        return true;
    }

    public MongoRecordReader(MongoInputSplit split) {
        _cursor = split.getCursor();
    }

    private BSONObject _current;
    private final DBCursor _cursor;
    private float _seen = 0;
    private float _total;

    private static final Log LOG = LogFactory.getLog( MongoRecordReader.class );

}
