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

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.bson.*;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.mapred.input.*;
import com.mongodb.hadoop.io.*;

@SuppressWarnings("deprecation")
public class MongoInputFormat implements InputFormat<ObjectWritable, BSONWritable> {
    
    private com.mongodb.hadoop.MongoInputFormat mif = new com.mongodb.hadoop.MongoInputFormat();

    public RecordReader<ObjectWritable, BSONWritable> getRecordReader(InputSplit split,
                                                                      JobConf job,
                                                                      Reporter reporter) {
        if (!(split instanceof com.mongodb.hadoop.input.MongoInputSplit))
            throw new IllegalStateException("Creation of a new RecordReader requires a MongoInputSplit instance.");

        final com.mongodb.hadoop.input.MongoInputSplit mis = (com.mongodb.hadoop.input.MongoInputSplit) split;

        return //(RecordReader<ObjectWritable, BSONWritable>) 
                new com.mongodb.hadoop.input.MongoRecordReader(mis);
    }

    public InputSplit[] getSplits(JobConf job, int numSplits) {
        List list =   mif.getSplits(  job, new MongoConfig(job));
        InputSplit[] ans = (InputSplit[]) list.toArray(new InputSplit[list.size()]);
        return ans;
    }

    public boolean verifyConfiguration(Configuration conf) {
        return true;
    }

    private static final Log log = LogFactory.getLog(MongoInputFormat.class);

}
