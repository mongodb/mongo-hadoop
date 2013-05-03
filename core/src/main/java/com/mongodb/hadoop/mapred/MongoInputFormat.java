// MongoImportFormat.java
/*
 * Copyright 2010 10gen Inc.
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

public class MongoInputFormat implements InputFormat<BSONWritable, BSONWritable> {


    @SuppressWarnings("deprecation") 
    public org.apache.hadoop.mapred.RecordReader<BSONWritable, BSONWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) {
        if (!(split instanceof MongoInputSplit))
            throw new IllegalStateException("Creation of a new RecordReader requires a MongoInputSplit instance.");

        final MongoInputSplit mis = (MongoInputSplit) split;

        return new MongoRecordReader(mis);
    }

    public InputSplit[] getSplits(JobConf job, int numSplits) {
        final MongoConfig conf = new MongoConfig(job);
        // TODO - Support allowing specification of numSplits to affect our ops?
        final List<org.apache.hadoop.mapreduce.InputSplit> splits = MongoSplitter.calculateSplits( conf );
        return splits.toArray(new InputSplit[0]);
    }

    public boolean verifyConfiguration(Configuration conf) {
        return true;
    }

    private static final Log log = LogFactory.getLog(MongoInputFormat.class);

}
