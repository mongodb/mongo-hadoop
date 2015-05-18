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

package com.mongodb.hadoop.mapred.input;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.LazyBSONCallback;
import org.bson.LazyBSONDecoder;

import java.io.IOException;

import static java.lang.String.format;

public class BSONFileRecordReader implements RecordReader<NullWritable, BSONWritable> {
    private static final Log LOG = LogFactory.getLog(BSONFileRecordReader.class);
    public static final long BSON_RR_POSITION_NOT_GIVEN = -1L;

    private FileSplit fileSplit;
    private FSDataInputStream in;
    private int numDocsRead = 0;
    private boolean finished = false;
    private long startingPosition = 0;
    private long pos = 0;

    private BSONCallback callback;
    private BSONDecoder decoder;

    public BSONFileRecordReader() {
        this(BSON_RR_POSITION_NOT_GIVEN);
    }

    public BSONFileRecordReader(final long startingPosition) {
        this.startingPosition = startingPosition;
    }

    /**
     * Initialize the RecordReader with an InputSplit and a Configuration.
     * Note that this method is not called from within Hadoop as in
     * {@link com.mongodb.hadoop.input.BSONFileRecordReader}; it exists for
     * consistency.
     *
     * @param inputSplit the FileSplit over which to iterate BSONWritables.
     * @param conf the job's Configuration.
     * @throws IOException
     */
    public void initialize(final InputSplit inputSplit, final Configuration conf) throws IOException {
        fileSplit = (FileSplit) inputSplit;
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(conf);
        in = fs.open(file, 16 * 1024 * 1024);
        pos = in.skip(startingPosition == BSON_RR_POSITION_NOT_GIVEN
          ? fileSplit.getStart() : startingPosition);
        if (MongoConfigUtil.getLazyBSON(conf)) {
            callback = new LazyBSONCallback();
            decoder = new LazyBSONDecoder();
        } else {
            callback = new BasicBSONCallback();
            decoder = new BasicBSONDecoder();
        }
    }

    @Override
    public boolean next(final NullWritable key, final BSONWritable value) throws IOException {
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
            BSONObject bo = (BSONObject) callback.get();
            value.setDoc(bo);

            numDocsRead++;
            if (LOG.isDebugEnabled() && numDocsRead % 5000 == 0) {
                LOG.debug(String.format("read %d docs from %s at %d", numDocsRead, fileSplit, in.getPos()));
            }
            return true;
        } catch (final Exception e) {
            LOG.error(format("Error reading key/value from bson file on line %d: %s", numDocsRead, e.getMessage()));
            try {
                close();
            } catch (final Exception e2) {
                LOG.warn(e2.getMessage(), e2);
            }
            return false;
        }
    }

    @Override
    public float getProgress() throws IOException {
        if (finished) {
            return 1f;
        }
        if (in != null) {
            return Math.min(
              1.0f, (pos - startingPosition) / (float) fileSplit.getLength());
        }
        return 0f;
    }

    @Override
    public long getPos() throws IOException {
        if (in != null) {
            return pos;
        }
        return startingPosition;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public BSONWritable createValue() {
        return new BSONWritable();
    }

    @Override
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("closing bson file split.");
        }
        finished = true;
        if (in != null) {
            in.close();
        }
    }

}
