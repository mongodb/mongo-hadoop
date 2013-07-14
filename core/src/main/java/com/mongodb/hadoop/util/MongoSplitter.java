package com.mongodb.hadoop.util;

import java.util.*;
import com.mongodb.*;
import com.mongodb.hadoop.input.*;
import org.bson.*;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.*;

public abstract class MongoSplitter {

    protected Configuration conf;

    public MongoSplitter(Configuration conf){
        this.conf = conf;
    }

    public abstract List<InputSplit> calculateSplits() throws SplitFailedException;

}
