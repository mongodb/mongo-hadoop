// MongoInputSplit.java

package com.mongodb.hadoop.input;

import java.io.*;
import java.util.*;

import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MongoInputSplit extends InputSplit implements Writable {

    public MongoInputSplit(){
        _config = null;
    }

    public MongoInputSplit( MongoConfig config ){
        _config = config;
    }
    
    public long getLength(){
        // TODO
        return 100;
    }
    
    public String[] getLocations(){
        // TODO
        return new String[]{};// "localhost" };
    }
    
    public void readFields(DataInput in)
        throws IOException {
        if ( _config != null )
            throw new IllegalStateException( "_config should be null when readFields called" );
        _config = new MongoConfig( in );
    }
    
    public void write(DataOutput out)
        throws IOException {
        _config.write( out );
    }

    MongoConfig getConfig(){
        return _config;
    }

    DBCursor cursor(){
        return _config.collection().find().limit( _config.limit() );
    }

    private MongoConfig _config;
}


