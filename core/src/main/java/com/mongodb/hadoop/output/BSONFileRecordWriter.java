/*
 * Copyright 2011-2013 10gen Inc.
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

package com.mongodb.hadoop.output;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;

import java.io.IOException;


public class BSONFileRecordWriter<K, V> extends RecordWriter<K, V> {

    private BSONEncoder bsonEnc = new BasicBSONEncoder();
    private FSDataOutputStream outFile = null;
    private FSDataOutputStream splitsFile = null;
    private long bytesWritten = 0L;
    private long currentSplitLen = 0;
    private long currentSplitStart = 0;
    private long splitSize;

    public BSONFileRecordWriter(final FSDataOutputStream outFile, final FSDataOutputStream splitsFile, final long splitSize) {
        this.outFile = outFile;
        this.splitsFile = splitsFile;
        this.splitSize = splitSize;

    }

    public BSONFileRecordWriter(final FSDataOutputStream outFile) {
        this(outFile, null, 0);
    }

    public void close(final TaskAttemptContext context) throws IOException {
        if (this.outFile != null) {
            this.outFile.close();
        }
        writeSplitData(0, true);
        if (this.splitsFile != null) {
            this.splitsFile.close();
        }
    }

    public void write(final K key, final V value) throws IOException {
        final FSDataOutputStream destination = this.outFile;

        if (value instanceof MongoUpdateWritable) {
            throw new IllegalArgumentException("MongoUpdateWriteable can only be used to output to a mongo collection, "
                                               + "not a static BSON file.");
        }

        Object keyBSON = null;
        BSONObject toEncode = null;
        byte[] outputByteBuf;
        if (key != null) {
            keyBSON = BSONWritable.toBSON(key);
            if (keyBSON != null) {
                toEncode = new BasicDBObject();
            }
        }

        if (value instanceof BSONWritable) {
            if (toEncode != null) {
                toEncode.putAll(((BSONWritable) value).getDoc());
            } else {
                toEncode = ((BSONWritable) value).getDoc();
            }
        } else if (value instanceof BSONObject) {
            if (toEncode != null) {
                toEncode.putAll((BSONObject) value);
            } else {
                toEncode = (BSONObject) value;
            }
        } else {
            if (toEncode != null) {
                toEncode.put("value", BSONWritable.toBSON(value));
            } else {
                final DBObject o = new BasicDBObject();
                o.put("value", BSONWritable.toBSON(value));
                toEncode = o;
            }
        }

        if (keyBSON != null) {
            toEncode.put("_id", keyBSON);
        }

        outputByteBuf = bsonEnc.encode(toEncode);
        destination.write(outputByteBuf, 0, outputByteBuf.length);
        bytesWritten += outputByteBuf.length;
        writeSplitData(outputByteBuf.length, false);
    }

    private void writeSplitData(final int docSize, final boolean force) throws IOException {
        //If no split file is being written, bail out now
        if (this.splitsFile == null) {
            return;
        }

        // hit the threshold of a split, write it to the metadata file
        if (force || currentSplitLen + docSize >= this.splitSize) {
            BSONObject splitObj = BasicDBObjectBuilder.start()
                                                      .add("s", currentSplitStart)
                                                      .add("l", currentSplitLen).get();
            byte[] encodedObj = this.bsonEnc.encode(splitObj);
            this.splitsFile.write(encodedObj, 0, encodedObj.length);

            //reset the split len and start
            this.currentSplitLen = 0;
            this.currentSplitStart = bytesWritten - docSize;
        } else {
            // Split hasn't hit threshold yet, just add size
            this.currentSplitLen += docSize;
        }
    }

}

