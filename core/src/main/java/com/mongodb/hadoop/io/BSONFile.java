package com.mongodb.hadoop.io;

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

import com.mongodb.hadoop.MongoOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import java.io.DataOutputStream;
import java.io.IOException;
import java.rmi.server.UID;
import java.security.MessageDigest;

public class BSONFile {



    /**
     * Write key/value pairs to a sequence-format file.
     */
    public static class Writer<K, V> implements java.io.Closeable {
        Configuration conf;
        FSDataOutputStream out;
        boolean ownOutputStream = true;
        DataOutputBuffer buffer = new DataOutputBuffer();

        /**
         * Implicit constructor: needed for the period of transition!
         */
        Writer() {
        }

        public Writer(FileSystem fs, Configuration conf, Path name) throws IOException {
            this(fs, conf, name, fs.getConf().getInt("io.file.buffer.size", 4096),
                    fs.getDefaultReplication(), fs.getDefaultBlockSize(), null);
        }

        public Writer(FileSystem fs, Configuration conf, Path name, Progressable progress) throws IOException {
            this(fs, conf, name, fs.getConf().getInt("io.file.buffer.size", 4096),
                    fs.getDefaultReplication(), fs.getDefaultBlockSize(), progress);
        }
        /**
         * Create the named file with write-progress reporter.
         */
        public Writer(FileSystem fs, Configuration conf, Path name,
                      int bufferSize, short replication, long blockSize,
                      Progressable progress)
                throws IOException {
            init(name, conf, fs.create(name, true, bufferSize, replication, blockSize, progress));

        }

        /**
         * Initialize.
         */
        @SuppressWarnings("unchecked")
        void init(Path name, Configuration conf, FSDataOutputStream out) throws IOException {
            this.conf = conf;
            this.out = out;
        }

        /**
         * create a sync point
         */
        public void sync() throws IOException {
            // TODO - See if we need to do anything here
        }

        /**
         * Returns the configuration of this file.
         */
        Configuration getConf() {
            return conf;
        }

        /**
         * Close the file.
         */
        public synchronized void close() throws IOException {
            if (out != null) {

                // Close the underlying stream iff we own it...
                if (ownOutputStream) {
                    out.close();
                } else {
                    out.flush();
                }
                out = null;
            }
        }

        synchronized void checkAndWriteSync() throws IOException {
            sync();
        }


        /**
         * Append a key/value pair.
         */
        @SuppressWarnings("unchecked")
        public synchronized void append(K key, V value)
                throws IOException {

            final BSONWritable o = new BSONWritable();

            buffer.reset();

/*            int keyLength = buffer.getLength();
            if (keyLength < 0)
                throw new IOException("negative length keys not allowed: " + key);*/

            if ( key instanceof BSONObject ) {
                o.put( "_id", key );
            }
            else {
                o.put( "_id", BSONWritable.toBSON( key ) );
            }

            if ( value instanceof BSONObject ) {
                o.putAll( (BSONObject) value );
            }
            else {
                o.put( "value", BSONWritable.toBSON( value ) );
            }

            // If _id is null remove it & Gen a new one, we don't want to override with null _id
            if (o.get("_id") == null) {
                o.removeField("_id");
                o.put( "_id", new ObjectId() );
            }

            o.write( out );
        }

        /**
         * Returns the current length of the output file.
         * <p/>
         * <p>This always returns a synchronized position.  In other words,
         * immediately after calling {@link SequenceFile.Reader#seek(long)} with a position
         * returned by this method, {@link SequenceFile.Reader#next(Writable)} may be called.  However
         * the key may be earlier in the file than key last written when this
         * method was called (e.g., with block-compression, it may be the first key
         * in the block that was being written when this method was called).
         */
        public synchronized long getLength() throws IOException {
            return out.getPos();
        }

    } // class Writer


    private static final Log log = LogFactory.getLog(BSONFile.class);

}
