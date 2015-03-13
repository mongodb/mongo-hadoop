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

package com.mongodb.hadoop.input;

import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoPathRetriever;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.LazyBSONCallback;
import org.bson.LazyBSONDecoder;

import java.io.IOException;

import static java.lang.String.format;

/**
 * <p>
 * Copyright (c) 2008 - 2013 10gen, Inc. (http://10gen.com)
 * </p>
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 * </p>
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * </p>
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * </p>
 */

public class BSONFileRecordReader extends RecordReader<Object, BSONObject> {
    private static final Log LOG = LogFactory.getLog(BSONFileRecordReader.class);

    private FileSplit fileSplit;
    private BSONObject value;
    private FSDataInputStream in;
    private int numDocsRead = 0;
    private boolean finished = false;

    private BSONCallback callback;
    private BSONDecoder decoder;

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) inputSplit;
        final Configuration configuration = context.getConfiguration();
        if (LOG.isDebugEnabled()) {
            LOG.debug("reading split " + fileSplit);
        }
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(configuration);
        in = fs.open(file, 16 * 1024 * 1024);
        in.seek(fileSplit.getStart());

        if (MongoConfigUtil.getLazyBSON(configuration)) {
            callback = new LazyBSONCallback();
            decoder = new LazyBSONDecoder();
        } else {
            callback = new BasicBSONCallback();
            decoder = new BasicBSONDecoder();
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            if (in.getPos() >= fileSplit.getStart() + fileSplit.getLength()) {
                try {
                    close();
                } catch (final Exception e) {
                    LOG.warn(e.getMessage(), e);
                }
                return false;
            }

            callback.reset();
            decoder.decode(in, callback);
            value = (BSONObject) callback.get();
            numDocsRead++;
            if (numDocsRead % 10000 == 0) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("read %d docs from %s at %d", numDocsRead, fileSplit, in.getPos()));
                }
            }
            return true;
        } catch (final Exception e) {
            LOG.error(format("Error reading key/value from bson file on line %d: %s", numDocsRead, e.getMessage()));
            try {
                close();
            } catch (final Exception e2) {
                LOG.warn(e.getMessage(), e);
            }
            return false;
        }
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        Object key = null;
        if (fileSplit instanceof BSONFileSplit) {
            key = MongoPathRetriever.get(
              value, ((BSONFileSplit) fileSplit).getKeyField());
        }
        return key != null ? key : NullWritable.get();
    }

    @Override
    public BSONObject getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (finished) {
            return 1f;
        }
        if (in != null) {
            return (float) (in.getPos() - fileSplit.getStart()) / fileSplit.getLength();
        }
        return 0f;
    }

    @Override
    public void close() throws IOException {
        finished = true;
        if (in != null) {
            in.close();
        }
    }

}
