package com.mongodb.hadoop.input;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.BSONLoader;
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
import org.bson.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class BSONFileRecordReader extends RecordReader<NullWritable, BSONWritable> {
    private FileSplit fileSplit;
    private Configuration conf;
    private BSONLoader rdr;
    private static final Log log = LogFactory.getLog(BSONFileRecordReader.class);
    private Object key;
    private BSONWritable value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = context.getConfiguration();
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream in = null;
        in = fs.open(file);
        rdr = new BSONLoader(in);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (rdr.hasNext()) {
            value = new BSONWritable( rdr.next() );
            return true;
        } else {
            return false;
        }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BSONWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return rdr.hasNext() ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

}
