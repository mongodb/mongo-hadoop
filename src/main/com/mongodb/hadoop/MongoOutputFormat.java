// MongoOutputFormat.java

package com.mongodb.hadoop;

import java.io.*;

import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.output.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoOutputFormat<K,V> extends OutputFormat<K,V> {
    private static final Log log = LogFactory.getLog(MongoOutputFormat.class);

    public MongoOutputFormat(){
    }
    
    public void checkOutputSpecs(JobContext context){
    }
    
    public OutputCommitter getOutputCommitter(TaskAttemptContext context){
        return new MongoOutputCommiter();
    }
    
    
    public RecordWriter<K,V> getRecordWriter(TaskAttemptContext context){
        return new MongoRecordWriter( MongoConfigUtil.getOutputCollection(context.getConfiguration()), context );
    }

}
