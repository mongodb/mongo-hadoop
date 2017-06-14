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

import com.mongodb.hadoop.input.BSONFileSplit;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;

public class BSONFileRecordReader implements RecordReader<NullWritable, BSONWritable> {
    public static final long BSON_RR_POSITION_NOT_GIVEN = -1L;

    private final com.mongodb.hadoop.input.BSONFileRecordReader delegate;
    private long pos = 0;

    public BSONFileRecordReader() {
        this(BSON_RR_POSITION_NOT_GIVEN);
    }

    public BSONFileRecordReader(final long startingPosition) {
        delegate =
          new com.mongodb.hadoop.input.BSONFileRecordReader(startingPosition);
    }

    /**
     * Initialize the RecordReader with an InputSplit and a Configuration.
     * Note that this method is not called from within Hadoop as in
     * {@link com.mongodb.hadoop.input.BSONFileRecordReader}; it exists for
     * consistency.
     *
     * @param inputSplit the FileSplit over which to iterate BSONWritables.
     * @param conf the job's Configuration.
     * @throws IOException when there is an error opening the file
     */
    public void initialize(final InputSplit inputSplit, final Configuration conf)
      throws IOException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        try {
            delegate.initialize(
              new BSONFileSplit(
                fileSplit.getPath(), fileSplit.getStart(),
                fileSplit.getLength(), fileSplit.getLocations()),
              new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next(final NullWritable key, final BSONWritable value)
      throws IOException {
        try {
            boolean result = delegate.nextKeyValue();
            value.setDoc(delegate.getCurrentValue());
            pos++;
            return result;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public float getProgress() throws IOException {
        try {
            return delegate.getProgress();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getPos() throws IOException {
        return pos;
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
        delegate.close();
    }

}
