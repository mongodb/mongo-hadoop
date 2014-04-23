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

package com.mongodb.hadoop;

// Mongo

import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.MongoSplitterFactory;
import com.mongodb.hadoop.splitter.SplitFailedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.List;

public class MongoInputFormat extends InputFormat<Object, BSONObject> {
    private static final Log LOG = LogFactory.getLog(MongoInputFormat.class);

    @Override
    public RecordReader<Object, BSONObject> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        if (!(split instanceof MongoInputSplit)) {
            throw new IllegalStateException("Creation of a new RecordReader requires a MongoInputSplit instance.");
        }

        final MongoInputSplit mis = (MongoInputSplit) split;

        return new com.mongodb.hadoop.input.MongoRecordReader(mis);
    }

    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException {
        final Configuration conf = context.getConfiguration();
        try {
            MongoSplitter splitterImpl = MongoSplitterFactory.getSplitter(conf);
            LOG.debug("Using " + splitterImpl.toString() + " to calculate splits.");
            return splitterImpl.calculateSplits();
        } catch (SplitFailedException spfe) {
            throw new IOException(spfe);
        }
    }

    public boolean verifyConfiguration(final Configuration conf) {
        return true;
    }
}
