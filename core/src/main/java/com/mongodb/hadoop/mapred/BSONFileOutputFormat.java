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

import com.mongodb.hadoop.mapred.output.BSONFileRecordWriter;
import com.mongodb.hadoop.splitter.BSONSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class BSONFileOutputFormat<K, V> extends FileOutputFormat<K, V> {

    public RecordWriter<K, V> getRecordWriter(final FileSystem ignored, final JobConf job, final String name,
                                              final Progressable progress) throws IOException {
        Path outPath = getDefaultWorkFile(job, name, ".bson");
        LOG.info("output going into " + outPath);

        FileSystem fs = outPath.getFileSystem(job);
        FSDataOutputStream outFile = fs.create(outPath);

        FSDataOutputStream splitFile = null;
        if (MongoConfigUtil.getBSONOutputBuildSplits(job)) {
            Path splitPath = new Path(outPath.getParent(), "." + outPath.getName() + ".splits");
            splitFile = fs.create(splitPath);
        }

        long splitSize = BSONSplitter.getSplitSize(job, null);

        return new BSONFileRecordWriter<K, V>(outFile, splitFile, splitSize);
    }

    public static Path getDefaultWorkFile(final JobConf conf, final String name, final String extension) {
        return new Path(getWorkOutputPath(conf), getUniqueName(conf, name) + extension);
    }

    private static final Log LOG = LogFactory.getLog(BSONFileOutputFormat.class);
}

