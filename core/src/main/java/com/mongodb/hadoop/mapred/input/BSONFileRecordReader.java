package com.mongodb.hadoop.mapred.input;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
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

public class BSONFileRecordReader implements RecordReader<NullWritable, BSONWritable> {
    private FileSplit fileSplit;
    private BSONReader rdr;
    private static final Log log = LogFactory.getLog(BSONFileRecordReader.class);
    private Object key;
    private BSONWritable value;
    private Configuration conf;

    public BSONFileRecordReader(Configuration conf, FileSplit fileSplit) throws IOException {
        this.fileSplit = fileSplit;
        this.conf = conf;
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream in = fs.open(file);
        rdr = new BSONReader(in);
    }

    protected boolean nextKeyValue() throws IOException {
        if (rdr.hasNext()) {
            value = new BSONWritable( rdr.next() );
            return true;
        } else {
            return false;
        }
    }

    public float getProgress() throws IOException {
        return rdr.hasNext() ? 1.0f : 0.0f;
    }


    public boolean next(NullWritable key, BSONWritable value) throws IOException {
        if ( nextKeyValue() ){
            log.trace( "Had another k/v" );
            value.putAll( this.value );
            return true;
        }
        else{
            log.info( "Cursor exhausted." );
            value = null;
            return false;
        }
    }

    public NullWritable createKey() {
        return NullWritable.get();
    }

    public BSONWritable createValue() {
        return new BSONWritable();
    }

    public long getPos() throws IOException {
        return 0;
    }

    public void close() throws IOException {
        // do nothing
    }

    private class BSONReader implements Iterable<BSONObject>, Iterator<BSONObject> {

        public BSONReader(final InputStream input) {
            _input = new DataInputStream( input );
        }

        public Iterator<BSONObject> iterator(){
            return this;
        }

        public boolean hasNext(){
            checkHeader();
            return hasMore.get();
        }

        private synchronized void checkHeader(){
            // Read the BSON length from the start of the record
            byte[] l = new byte[4];
            try {
                _input.readFully( l );
                nextLen = org.bson.io.Bits.readInt( l );
                nextHdr = l;
                hasMore.set( true );
            } catch (Exception e) {
                log.debug( "Failed to get next header: " + e, e );
                hasMore.set( false );
                try {
                    _input.close();
                }
                catch ( IOException e1 ) { }
            }
        }

        public BSONObject next(){
            try {
                byte[] data = new byte[nextLen + 4];
                System.arraycopy( nextHdr, 0, data, 0, 4 );
                _input.readFully( data, 4, nextLen - 4 );
                decoder.decode( data, callback );
                return (BSONObject) callback.get();
            }
            catch ( IOException e ) {
                /* If we can't read another length it's not an error, just return quietly. */
                log.info( "No Length Header available." + e );
                hasMore.set( false );
                try {
                    _input.close();
                }
                catch ( IOException e1 ) { }
                throw new NoSuchElementException("Iteration completed.");
            }
        }

        public void remove(){
            throw new UnsupportedOperationException();
        }

        private final BSONDecoder decoder = new BasicBSONDecoder();
        private final BSONCallback callback = new BasicBSONCallback();

        private volatile byte[] nextHdr;
        private volatile int nextLen;
        private AtomicBoolean hasMore = new AtomicBoolean( true );
        private final DataInputStream _input;
    }
}
