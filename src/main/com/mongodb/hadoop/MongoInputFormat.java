// MongoImportFormat.java

package com.mongodb.hadoop;

import java.io.*;
import java.util.*;

import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MongoInputFormat extends InputFormat<Object,BSONObject> {


    public RecordReader<Object,BSONObject> createRecordReader(InputSplit split, TaskAttemptContext context){
        if ( ! ( split instanceof MongoInputSplit ) )
            throw new IllegalStateException( "need a MongoInputSplit" );

        MongoInputSplit mis = (MongoInputSplit)split;

        return new MongoRecordReader( mis );
    }
    
    public List<InputSplit> getSplits(JobContext context){
        _config = new MongoConfig( context , MongoConfig.INPUT );
        
        List<InputSplit> l = new ArrayList<InputSplit>();
        l.add( new MongoInputSplit( _config ) );
        return l;
    }

    private MongoConfig _config;
}


