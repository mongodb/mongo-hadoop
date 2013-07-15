/*
 * Copyright 2011 10gen Inc.
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

import com.mongodb.*;
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.bson.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.*;


public class BSONFileRecordWriter<K, V> extends RecordWriter<K, V> {
    
    private static final Log log = LogFactory.getLog( BSONFileRecordWriter.class );
    private BSONEncoder bsonEnc = new BasicBSONEncoder();
    private boolean outFileOpened = false;
    private FSDataOutputStream outFile = null;
    private FSDataOutputStream splitsFile = null;
    private long bytesWritten = 0L;
    private long currentSplitLen = 0;
    private long currentSplitStart = 0;
    private long splitSize;

    public BSONFileRecordWriter(FSDataOutputStream outFile, FSDataOutputStream splitsFile, long splitSize){
        this.outFile = outFile;
        this.splitsFile = splitsFile;
        this.splitSize = splitSize;

    }

    public BSONFileRecordWriter(FSDataOutputStream outFile){
        this(outFile, null, 0);
    }

    public void close( TaskAttemptContext context ) throws IOException{
        if( this.outFile != null){
            this.outFile.close();
        }
        writeSplitData(0, true);
        if(this.splitsFile != null){
            this.splitsFile.close();
        }
    }

    public void write( K key, V value ) throws IOException{
        final FSDataOutputStream destination = this.outFile;

        if( value instanceof MongoUpdateWritable ){
            throw new IllegalArgumentException("MongoUpdateWriteable can only be used to output to a mongo collection, not a static BSON file.");
        }

        Object keyBSON = null;
        BSONObject toEncode = null;
        byte[] outputByteBuf;
        if(key != null){
            keyBSON = BSONWritable.toBSON(key);
            if(keyBSON != null){
                toEncode = new BasicDBObject();
            }
        }

        if (value instanceof BSONWritable ){
            if(toEncode != null){
                ((BasicDBObject)toEncode).putAll(((BSONWritable)value).getDoc());
            }else{
                toEncode = ((BSONWritable)value).getDoc();
            }
        }else if ( value instanceof BSONObject ){
            if(toEncode != null){
                ((BasicDBObject)toEncode).putAll((BSONObject)value);
            }else{
                toEncode = (BSONObject)value;
            }
        }else{
            if(toEncode != null){
                ((BasicDBObject)toEncode).put("value", BSONWritable.toBSON( value ));
            }else{
                final DBObject o = new BasicDBObject();
                o.put( "value", BSONWritable.toBSON( value ) );
                toEncode = o;
            }
        }

        if( keyBSON != null ){
            ((BasicDBObject)toEncode).put("_id", keyBSON);
        }

        outputByteBuf = bsonEnc.encode(toEncode);
        destination.write(outputByteBuf, 0, outputByteBuf.length);
        bytesWritten += outputByteBuf.length;
        writeSplitData(outputByteBuf.length, false);
    }

    private void writeSplitData(int docSize, boolean force) throws IOException{
        //If no split file is being written, bail out now
        if(this.splitsFile == null)
            return;

        // hit the threshold of a split, write it to the metadata file
        if(force || currentSplitLen + docSize >= this.splitSize){
            BSONObject splitObj = BasicDBObjectBuilder.start()
                                    .add( "s" , currentSplitStart)
                                    .add( "l" , currentSplitLen).get();
            byte[] encodedObj = this.bsonEnc.encode(splitObj);
            this.splitsFile.write(encodedObj, 0, encodedObj.length);

            //reset the split len and start
            this.currentSplitLen = 0;
            this.currentSplitStart = bytesWritten - docSize;
        }else{
            // Split hasn't hit threshold yet, just add size
            this.currentSplitLen += docSize;
        }
    }

}

