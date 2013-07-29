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
import com.mongodb.hadoop.util.BSONLoader;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.FileSplit;
import org.bson.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BSONFileRecordReader implements RecordReader<NullWritable, BSONWritable> {
    private FileSplit fileSplit;
    private Configuration conf;
    private BSONLoader rdr;
    private static final Log log = LogFactory.getLog(BSONFileRecordReader.class);
    private Object key;
    private BSONWritable value;
    byte[] headerBuf = new byte[4];
    private FSDataInputStream in;
    private int numDocsRead = 0;
    private boolean finished = false;

    private BSONCallback callback;
    private BSONDecoder decoder;

    public BSONFileRecordReader(){ }

    public void initialize(InputSplit inputSplit, Configuration conf) throws IOException {
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = conf;
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(conf);
        in = fs.open(file, 16*1024*1024);
        in.seek(fileSplit.getStart());
        if (MongoConfigUtil.getLazyBSON(conf)) {
            callback = new LazyBSONCallback();
            decoder = new LazyBSONDecoder();
        } else {
            callback = new BasicBSONCallback();
            decoder = new BasicBSONDecoder();
        }
    }

    @Override
    public boolean next(NullWritable key, BSONWritable value) throws IOException {
        try{
            if(in.getPos() >= this.fileSplit.getStart() + this.fileSplit.getLength()){
                try{
                    this.close();
                }catch(Exception e){
                }finally{
                    return false;
                }
            }

            callback.reset();
            int bytesRead = decoder.decode(in, callback);
            BSONObject bo = (BSONObject)callback.get();
            value.setDoc(bo);

            numDocsRead++;
            if(numDocsRead % 5000 == 0){
                log.debug("read " + numDocsRead + " docs from " + this.fileSplit.toString() + " at " + in.getPos());
            }
            return true;
        }catch(Exception e){
            log.error("Error reading key/value from bson file: " + e.getMessage());
            try{
                this.close();
            }catch(Exception e2){
            }finally{
                return false;
            }
        }
    }

    public float getProgress() throws IOException {
        if(this.finished)
            return 1f;
        if(in != null)
            return new Float(in.getPos() - this.fileSplit.getStart()) / this.fileSplit.getLength();
        return 0f;
    }

    public long getPos() throws IOException {
        if(this.finished)
            return this.fileSplit.getStart() + this.fileSplit.getLength();
        if(in != null )
            return in.getPos();
        return this.fileSplit.getStart();
    }

    @Override
    public NullWritable createKey(){
        return NullWritable.get();
    }

    @Override
    public BSONWritable createValue() {
        return new BSONWritable();
    }

    @Override
    public void close() throws IOException {
        log.info("closing bson file split.");
        this.finished = true;
        if(this.in != null){
            in.close();
        }
    }

}
