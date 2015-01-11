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

import com.mongodb.hadoop.output.BSONFileRecordWriter;
import com.mongodb.hadoop.splitter.BSONSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BSONFileOutputFormat<K, V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext context) throws IOException {
        // Open data output stream

        Path outPath = getDefaultWorkFile(context, ".bson");
        LOG.info("output going into " + outPath);

        FileSystem fs = outPath.getFileSystem(context.getConfiguration());
        FSDataOutputStream outFile = fs.create(outPath);

        FSDataOutputStream splitFile = null;
        if (MongoConfigUtil.getBSONOutputBuildSplits(context.getConfiguration())) {
            Path splitPath = new Path(outPath.getParent(), "." + outPath.getName() + ".splits");
            splitFile = fs.create(splitPath);
        }

        long splitSize = BSONSplitter.getSplitSize(context.getConfiguration(), null);
        return new BSONFileRecordWriter<K, V>(outFile, splitFile, splitSize);
    }

    private static final Log LOG = LogFactory.getLog(BSONFileOutputFormat.class);
}

