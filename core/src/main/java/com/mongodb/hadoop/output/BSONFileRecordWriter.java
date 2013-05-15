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
    private final Path outputPath;
    private boolean outFileOpened = false;
    private FSDataOutputStream outFile = null;
    private final TaskAttemptContext _context;

    public BSONFileRecordWriter( TaskAttemptContext ctx ){
        this.outputPath = new Path(ctx.getConfiguration().get("mapred.output.file"));
        this._context = ctx;
    }

    public void close( TaskAttemptContext context ) throws IOException{
        if( this.outFile != null){
            this.outFile.close();
        }
    }

    private FSDataOutputStream getOutputStream() throws IOException{
        if(this.outFile != null){
            return this.outFile;
        }else if(this.outFileOpened && this.outFile == null){
            throw new IllegalStateException("Opening of output file failed.");
        }else{
            FileSystem fs = this.outputPath.getFileSystem(this._context.getConfiguration());
            this.outFile = fs.create(this.outputPath);
            return this.outFile;
        }
    }

    public void write( K key, V value ) throws IOException{
        final FSDataOutputStream destination = getOutputStream();

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
    }

}

