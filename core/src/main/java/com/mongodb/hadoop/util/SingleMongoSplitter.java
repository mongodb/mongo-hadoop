package com.mongodb.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import com.mongodb.hadoop.input.MongoInputSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import com.mongodb.*;
import org.bson.*;
import java.util.*;

public class SingleMongoSplitter extends MongoCollectionSplitter{

    //Create a single split which consists of a single 
    //a query over the entire collection.

    public SingleMongoSplitter(Configuration conf, MongoURI inputURI){
        super(conf, inputURI);
    }

    @Override
    public List<InputSplit> calculateSplits(){
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
