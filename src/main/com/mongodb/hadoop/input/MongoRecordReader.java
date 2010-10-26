// MongoRecordReader.java

package com.mongodb.hadoop.input;

import org.bson.*;
import com.mongodb.*;

import org.apache.hadoop.mapreduce.*;

public class MongoRecordReader extends RecordReader<Object,BSONObject>{
        
    public MongoRecordReader( MongoInputSplit split ){
        _split = split;
        //try {
        _cursor = _split.getCursor();
            /*        }
        catch ( Exception e ){
            throw new RuntimeException( "blah" , e );
        }
            */
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

    public void initialize(InputSplit split, TaskAttemptContext context){
        if ( split != _split )
            throw new IllegalStateException( "split != _split ??? " );
        _total = _cursor.size();
    }
        
    public boolean nextKeyValue(){
        if ( ! _cursor.hasNext() )
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
