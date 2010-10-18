// MongoOutputFormat.java

package com.mongodb.hadoop;

import java.io.*;

import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.output.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class MongoOutputFormat<K,V> extends OutputFormat<K,V> {

    public MongoOutputFormat(){
    }
    
    public void checkOutputSpecs(JobContext context){
        // should check to make sure don't override here
        _init( context );
    }
    
    public OutputCommitter getOutputCommitter(TaskAttemptContext context){
        return new MongoOutputCommiter();
    }
    
    
    public RecordWriter<K,V> getRecordWriter(TaskAttemptContext context){
        _init( context );
        return new MongoRecordWriter( _config.collection(), context );
    }

    void _init( JobContext context ){
        if ( _config == null )
            _config = new MongoConfig( context , MongoConfig.OUTPUT );

        // TODO: should make sure its the same

    }

    MongoConfig _config;
}
