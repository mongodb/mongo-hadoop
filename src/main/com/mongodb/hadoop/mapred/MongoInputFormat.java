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

    public RecordReader<ObjectWritable, BSONWritable> getRecordReader(InputSplit split,
                                                                      JobConf job,
                                                                      Reporter reporter) {
        if (!(split instanceof MongoInputSplit))
            throw new IllegalStateException("Creation of a new RecordReader requires a MongoInputSplit instance.");

        final MongoInputSplit mis = (MongoInputSplit) split;

        return (RecordReader<ObjectWritable, BSONWritable>) new MongoRecordReader(mis);
    }

    public InputSplit[] getSplits(JobConf job, int numSplits) {
        final MongoConfig conf = new MongoConfig(job);

        if (conf.getLimit() > 0 || conf.getSkip() > 0)
        /**
         * TODO - If they specify skip or limit we create only one input
         * split
         */
            throw new IllegalArgumentException("skip() and limit() is not currently supported due to input split "
                                               + "issues.");
        else {
            /**
             * On the jobclient side we want *ONLY* the min and max ids for each
             * split; Actual querying will be done on the individual mappers.
             */
            /*final int splitSize = conf.getSplitSize();*/
            // For first release, no splits, no sharding
            InputSplit[] splits =
                    {(InputSplit) new MongoInputSplit(conf.getInputURI(), conf.getQuery(), conf.getFields(),
                                                      conf.getSort(), conf.getLimit(), conf.getSkip())};
            log.info("Calculated " + splits.length + " split objects.");
            return splits;
        }
    }

    public boolean verifyConfiguration(Configuration conf) {
        return true;
    }

    private static final Log log = LogFactory.getLog(MongoInputFormat.class);

}
