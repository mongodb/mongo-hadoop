/*
 * Copyright 2010-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.splitter;

import org.apache.hadoop.conf.Configuration;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.*;
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

    public SingleMongoSplitter(Configuration conf){
        super(conf);
    }

    @Override
    public List<InputSplit> calculateSplits(){
        init();

        MongoURI inputURI = MongoConfigUtil.getInputURI(conf);
        log.info("SingleMongoSplitter calculating splits for " + inputURI);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        MongoInputSplit mongoSplit = new MongoInputSplit();
    
        mongoSplit.setInputURI(MongoConfigUtil.getInputURI(conf));
        mongoSplit.setAuthURI(MongoConfigUtil.getAuthURI(conf));
        mongoSplit.setQuery(MongoConfigUtil.getQuery(conf));
        mongoSplit.setNoTimeout(MongoConfigUtil.isNoTimeout(conf));
        mongoSplit.setFields(MongoConfigUtil.getFields(conf));
        mongoSplit.setSort(MongoConfigUtil.getSort(conf));

        //Not using any index min/max bounds, so range query is 
        //meaningless here - don't set it
        //mongoSplit.setUseRangeQuery(...)


        splits.add(mongoSplit);
        return splits;
    }

}
