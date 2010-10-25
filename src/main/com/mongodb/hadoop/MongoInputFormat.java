// MongoImportFormat.java

package com.mongodb.hadoop;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MongoInputFormat extends InputFormat<Object,BSONObject> {
    private static final Log log = LogFactory.getLog(MongoInputFormat.class);

    public RecordReader<Object,BSONObject> createRecordReader(InputSplit split, TaskAttemptContext context){
        if ( ! ( split instanceof MongoInputSplit ) )
            throw new IllegalStateException( "need a MongoInputSplit" );

        MongoInputSplit mis = (MongoInputSplit)split;

        return new MongoRecordReader( mis );
    }
    
    public List<InputSplit> getSplits(JobContext context){
        List<InputSplit> l = new ArrayList<InputSplit>();
        l.add( new MongoInputSplit( context.getConfiguration() ) );
        return l;
    }

}


