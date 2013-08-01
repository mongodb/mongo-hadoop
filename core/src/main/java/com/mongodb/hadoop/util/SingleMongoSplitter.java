package com.mongodb.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import com.mongodb.hadoop.input.MongoInputSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import com.mongodb.*;
import org.bson.*;
import java.util.*;
import org.apache.commons.logging.*;

/* This implementation of MongoSplitter does not actually
 * do any splitting, it will just create a single input split
 * which represents the entire data set within a collection.
 */
public class SingleMongoSplitter extends MongoCollectionSplitter{

    private static final Log log = LogFactory.getLog( SingleMongoSplitter.class );

    //Create a single split which consists of a single 
    //a query over the entire collection.


    public SingleMongoSplitter(){ }

    public SingleMongoSplitter(Configuration conf, MongoURI inputURI){
        super(conf, inputURI);
    }

    @Override
    public List<InputSplit> calculateSplits(){
        log.info("SingleMongoSplitter calculating splits for " + this.inputURI);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        MongoInputSplit mongoSplit = new MongoInputSplit();
        mongoSplit.setInputURI(this.inputURI);
        mongoSplit.setQuery(this.query);
        mongoSplit.setFields(this.fields);
        mongoSplit.setSort(this.sort);
        mongoSplit.setNoTimeout(this.noTimeout);
        splits.add(mongoSplit);
        return splits;
    }

}
