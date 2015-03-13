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

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.List;

/* This implementation of MongoSplitter does not actually
 * do any splitting, it will just create a single input split
 * which represents the entire data set within a collection.
 */
public class SingleMongoSplitter extends MongoCollectionSplitter {

    private static final Log LOG = LogFactory.getLog(SingleMongoSplitter.class);

    //Create a single split which consists of a single 
    //a query over the entire collection.


    public SingleMongoSplitter() {
    }

    public SingleMongoSplitter(final Configuration conf) {
        super(conf);
    }

    @Override
    public List<InputSplit> calculateSplits() {
        MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
        LOG.info("SingleMongoSplitter calculating splits for " + inputURI);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        MongoInputSplit mongoSplit = new MongoInputSplit();

        mongoSplit.setInputURI(MongoConfigUtil.getInputURI(getConfiguration()));
        mongoSplit.setAuthURI(MongoConfigUtil.getAuthURI(getConfiguration()));
        mongoSplit.setQuery(MongoConfigUtil.getQuery(getConfiguration()));
        mongoSplit.setNoTimeout(MongoConfigUtil.isNoTimeout(getConfiguration()));
        mongoSplit.setFields(MongoConfigUtil.getFields(getConfiguration()));
        mongoSplit.setSort(MongoConfigUtil.getSort(getConfiguration()));
        mongoSplit.setKeyField(MongoConfigUtil.getInputKey(getConfiguration()));

        //Not using any index min/max bounds, so range query is 
        //meaningless here - don't set it
        //mongoSplit.setUseRangeQuery(...)


        splits.add(mongoSplit);
        return splits;
    }

}
