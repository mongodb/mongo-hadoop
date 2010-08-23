// MongoOutputCommiter.java

package com.mongodb.hadoop.output;

import java.io.*;

import org.bson.*;
import com.mongodb.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class MongoOutputCommiter extends OutputCommitter {
    
    public void abortTask(TaskAttemptContext taskContext){
        System.out.println( "should abort task" );
    }
    
    public void cleanupJob(JobContext jobContext){
        System.out.println( "should cleanup job" );
    }
    
    public void commitTask(TaskAttemptContext taskContext){
        System.out.println( "should commit task" );
    }
    
    public boolean needsTaskCommit(TaskAttemptContext taskContext){
        return true;
    }
    
    public void setupJob(JobContext jobContext){
        System.out.println( "should setup job" );
    }
    
    public void setupTask(TaskAttemptContext taskContext){
        System.out.println( "should setup context" );
    }
}
