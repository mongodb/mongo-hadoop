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

package com.mongodb.hadoop.mapred;

import java.util.*;

import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.mapred.input.MongoRecordReader;
import com.mongodb.hadoop.util.*;
import com.mongodb.hadoop.splitter.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.io.*;
import org.bson.BSONObject;
import com.mongodb.BasicDBObject;
import java.io.IOException;

public class MongoInputFormat implements InputFormat<BSONWritable, BSONWritable> {


    @SuppressWarnings("deprecation") 
    public org.apache.hadoop.mapred.RecordReader<BSONWritable, BSONWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) {
        if (!(split instanceof MongoInputSplit))
            throw new IllegalStateException("Creation of a new RecordReader requires a MongoInputSplit instance.");

        final MongoInputSplit mis = (MongoInputSplit) split;

        return new MongoRecordReader(mis);
    }

    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        try{
            MongoSplitter splitterImpl = MongoSplitterFactory.getSplitter(job);
            log.info("Using " + splitterImpl.toString() + " to calculate splits. (old mapreduce API)");
            final List<org.apache.hadoop.mapreduce.InputSplit> splits = splitterImpl.calculateSplits();
            return splits.toArray(new InputSplit[0]);
        }catch(SplitFailedException spfe){
            throw new IOException(spfe);
        }
    }

    public boolean verifyConfiguration(Configuration conf) {
        return true;
    }

    private static final Log log = LogFactory.getLog(MongoInputFormat.class);

}
