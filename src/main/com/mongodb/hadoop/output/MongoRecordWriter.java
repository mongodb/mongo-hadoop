// MongoRecordWriter.java

package com.mongodb.hadoop.output;

import java.io.*;

import org.bson.*;
import com.mongodb.*;
import com.mongodb.hadoop.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MongoRecordWriter<K,V> extends RecordWriter<K,V> {
        
    public MongoRecordWriter(DBCollection c, TaskAttemptContext ctx) {
        _collection = c;
        _context = ctx;
    }
    
    public void close(TaskAttemptContext context){
        _collection.getDB().getLastError();
    }
    
    Object toBSON( Object x ){
        if ( x == null )
            return null;
        if ( x instanceof Text )
            return x.toString();
        if ( x instanceof IntWritable )
            return ((IntWritable)x).get();
        throw new RuntimeException( "can't convert: " + x.getClass().getName() + " to BSON" );
    }
    
    public void write (K key, V value)
        throws IOException {
        DBObject o = new BasicDBObject();
        
        if ( key instanceof MongoOutput )
            ((MongoOutput)key).appendAsKey( o );
        else
            o.put( "_id" , toBSON(key) );

        if ( value instanceof MongoOutput )
            ((MongoOutput)value).appendAsValue( o );
        else
            o.put( "value" , toBSON( value ) );
        
        try {
            _collection.save( o );
        }
        catch ( MongoException e ){
            throw new IOException( "can't write to mongo" , e );
        }
    }
    
    public TaskAttemptContext getContext() {
      return _context;
    }

    final DBCollection _collection;
    final TaskAttemptContext _context;
}

