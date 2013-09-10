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

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.BSONLoader;
import com.mongodb.hadoop.util.MongoConfigUtil;
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
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.bson.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

public class BSONFileRecordReader extends RecordReader<NullWritable, BSONObject> {
    private FileSplit fileSplit;
    private Configuration conf;
    private BSONLoader rdr;
    private static final Log log = LogFactory.getLog(BSONFileRecordReader.class);
    private Object key;
    private BSONObject value;
    byte[] headerBuf = new byte[4];
    private InputStream in;
    private int numDocsRead = 0;
    private boolean finished = false;

    private BSONCallback callback;
    private BSONDecoder decoder;

    private CompressionCodecFactory compressionCodecs = null;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private Seekable filePosition;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = context.getConfiguration();
        Path file = fileSplit.getPath();
        compressionCodecs = new CompressionCodecFactory(this.conf);
        codec = compressionCodecs.getCodec(file);
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(file, 16*1024*1024);
        if (codec == null) {
            log.info("reading split " + this.fileSplit.toString());
            fileIn.seek(fileSplit.getStart());
            in = fileIn;
        } else {
            if (fileSplit.getStart() > 0) {
                throw new IOException("File is not seekable but start of split is non-zero");
            }
            decompressor = CodecPool.getDecompressor(codec);
            in = codec.createInputStream(fileIn, decompressor);
            log.info("reading compressed split " + this.fileSplit.toString());
            // start is ignored. as the file is not really seekable.
            // but then the splits should be starting at 0
        }
        filePosition = fileIn;

        if (MongoConfigUtil.getLazyBSON(this.conf)) {
            callback = new LazyBSONCallback();
            decoder = new LazyBSONDecoder();
        } else {
            callback = new BasicBSONCallback();
            decoder = new BasicBSONDecoder();
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try{
            if (filePosition.getPos() >= this.fileSplit.getStart() + this.fileSplit.getLength()
                    && (codec == null || in.available() == 0)) {
                try{
                    this.close();
                }catch(Exception e){
                }finally{
                    return false;
                }
            }

            callback.reset();
            int bytesRead = decoder.decode(in, callback);
            value = (BSONObject) callback.get();
            numDocsRead++;
            if(numDocsRead % 10000 == 0){
                log.debug("read " + numDocsRead + " docs from " + this.fileSplit.toString() + " at " + filePosition.getPos());
            }
            return true;
        }catch(Exception e){
            e.printStackTrace();
            log.warn("Error reading key/value from bson file: " + e.getMessage());
            try{
                this.close();
            }catch(Exception e2){
            }finally{
                return false;
            }
        }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BSONObject getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(this.finished)
            return 1f;
        if(in != null)
            return new Float(filePosition.getPos() - this.fileSplit.getStart()) / this.fileSplit.getLength();
        return 0f;
    }

    @Override
    public void close() throws IOException {
        this.finished = true;
        if(this.in != null){
            in.close();
        }
        if (codec != null){
            ((FSDataInputStream)this.filePosition).close();
        }
    }

}
