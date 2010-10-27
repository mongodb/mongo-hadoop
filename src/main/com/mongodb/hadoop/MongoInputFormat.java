// MongoImportFormat.java

package com.mongodb.hadoop;

import java.io.*;
import java.util.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.bson.*;

import org.bson.types.ObjectId;
import com.mongodb.*;

import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.util.MongoConfigUtil;

import com.mongodb.util.JSON;

public class MongoInputFormat extends InputFormat<Object,BSONObject> {
    private static final Log log = LogFactory.getLog(MongoInputFormat.class);

    public RecordReader<Object, BSONObject> createRecordReader(InputSplit split, TaskAttemptContext context){
        if ( ! ( split instanceof MongoInputSplit ) )
            throw new IllegalStateException("Creation of a new RecordReader requires a MongoInputSplit instance." );

        MongoInputSplit mis = (MongoInputSplit)split;

        return new MongoRecordReader( mis );
    }

    public List<InputSplit> getSplits(JobContext context) {
        MongoConfig conf = new MongoConfig(context.getConfiguration());

        if (conf.getLimit() > 0 || conf.getSkip() > 0) {
            /** TODO - If they specify skip or limit we create only one input split */
            throw new IllegalArgumentException("skip() and limit() is not currently supported do to input split issues.");
        } 
        else {
            /**
             * On the jobclient side we want *ONLY* the min and max ids
             * for each split;  Actual querying will be done on the individual
             * mappers.
             */
            int splitSize = conf.getSplitSize();
            List<InputSplit> splits = new ArrayList<InputSplit>(1);
            // For first release, no splits, no sharding
            splits.add(new MongoInputSplit(conf.getInputURI(), conf.getQuery(), conf.getFields(), conf.getSort(), conf.getLimit(), conf.getSkip()));
            log.info("Calculated " + splits.size() + " split objects.");
            return splits;
        }
    }

        
    public boolean verifyConfiguration(Configuration conf) {
        return true;
    }
}


