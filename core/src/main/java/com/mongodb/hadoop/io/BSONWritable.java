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

package com.mongodb.hadoop.io;

import com.mongodb.BasicDBObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * This is <em>not</em> reusable.
 */

@SuppressWarnings("deprecation")
public class BSONWritable implements WritableComparable {

    private static final byte[] HEX_CHAR = new byte[]{
                                                         '0', '1', '2', '3', '4', '5', '6', '7', '8',
                                                         '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };


    private static final Log LOG = LogFactory.getLog(BSONWritable.class);

    static {
        // register this comparator
        WritableComparator.define(BSONWritable.class, new BSONWritableComparator());
    }

    //CHECKSTYLE:OFF
    protected BSONObject _doc;
    //CHECKSTYLE:ON

    public BSONWritable() {
        _doc = new BasicBSONObject();
    }

    public BSONWritable(final BSONObject doc) {
        this();
        setDoc(doc);
    }

    public void setDoc(final BSONObject doc) {
        this._doc = doc;
    }

    public BSONObject getDoc() {
        return this._doc;
    }

    public Map toMap() {
        return _doc.toMap();
    }

    /**
     * {@inheritDoc}
     *
     * @see Writable#write(DataOutput)
     */
    public void write(final DataOutput out) throws IOException {
        BSONEncoder enc = new BasicBSONEncoder();
        BasicOutputBuffer buf = new BasicOutputBuffer();
        enc.set(buf);
        enc.putObject(_doc);
        enc.done();
        buf.pipe(out);
    }


    /**
     * {@inheritDoc}
     *
     * @see Writable#readFields(DataInput)
     */
    public void readFields(final DataInput in) throws IOException {
        BSONDecoder dec = new BasicBSONDecoder();
        BSONCallback cb = new BasicBSONCallback();
        // Read the BSON length from the start of the record
        byte[] l = new byte[4];
        try {
            in.readFully(l);
            int dataLen = Bits.readInt(l);
            if (LOG.isDebugEnabled()) {
                LOG.debug("*** Expected DataLen: " + dataLen);
            }
            byte[] data = new byte[dataLen + 4];
            System.arraycopy(l, 0, data, 0, 4);
            in.readFully(data, 4, dataLen - 4);
            dec.decode(data, cb);
            _doc = (BSONObject) cb.get();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Decoded a BSON Object: " + _doc);
            }
        } catch (Exception e) {
            /* If we can't read another length it's not an error, just return quietly. */
            // TODO - Figure out how to gracefully mark this as an empty
            LOG.info("No Length Header available." + e);
            _doc = new BasicDBObject();
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "<BSONWritable:" + this._doc.toString() + ">";
    }

    /**
     * Used by child copy constructors.
     *
     * @param other
     */
    protected synchronized void copy(final Writable other) {
        if (other != null) {
            try {
                DataOutputBuffer out = new DataOutputBuffer();
                other.write(out);
                DataInputBuffer in = new DataInputBuffer();
                in.reset(out.getData(), out.getLength());
                readFields(in);

            } catch (IOException e) {
                throw new IllegalArgumentException("map cannot be copied: " + e.getMessage());
            }

        } else {
            throw new IllegalArgumentException("source map cannot be null");
        }
    }

    public int compareTo(final Object o) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(" ************ Compare: '" + this + "' to '" + o + "'");
        }
        return new BSONWritableComparator().compare(this, o);
    }

    protected static void dumpBytes(final BasicOutputBuffer buf) {// Temp debug output
        dumpBytes(buf.toByteArray());
    }

    protected static void dumpBytes(final byte[] buffer) {
        StringBuilder sb = new StringBuilder(2 + (3 * buffer.length));

        for (byte b : buffer) {
            sb.append("0x").append((char) (HEX_CHAR[(b & 0x00F0) >> 4])).append(
                                                                                   (char) (HEX_CHAR[b & 0x000F])).append(" ");
        }

        LOG.info("Byte Dump: " + sb.toString());
    }

    public static Object toBSON(final Object x) {
        if (x == null) {
            return null;
        }
        if (x instanceof Text || x instanceof UTF8) {
            return x.toString();
        }
        if (x instanceof BSONWritable) {
            return ((BSONWritable) x).getDoc();
        }
        if (x instanceof Writable) {
            if (x instanceof AbstractMapWritable) {
                throw new IllegalArgumentException("ERROR: MapWritables are not presently supported for MongoDB Serialization.");
            }
            if (x instanceof ArrayWritable) { // TODO - test me
                Writable[] o = ((ArrayWritable) x).get();
                Object[] a = new Object[o.length];
                for (int i = 0; i < o.length; i++) {
                    a[i] = toBSON(o[i]);
                }
            }
            if (x instanceof NullWritable) {
                return null;
            }
            if (x instanceof BooleanWritable) {
                return ((BooleanWritable) x).get();
            }
            if (x instanceof BytesWritable) {
                return ((BytesWritable) x).getBytes();
            }
            if (x instanceof ByteWritable) {
                return ((ByteWritable) x).get();
            }
            if (x instanceof DoubleWritable) {
                return ((DoubleWritable) x).get();
            }
            if (x instanceof FloatWritable) {
                return ((FloatWritable) x).get();
            }
            if (x instanceof LongWritable) {
                return ((LongWritable) x).get();
            }
            if (x instanceof IntWritable) {
                return ((IntWritable) x).get();
            }

            // TODO - Support counters

        }
        throw new RuntimeException("can't convert: " + x.getClass().getName() + " to BSON");
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final BSONWritable other = (BSONWritable) obj;
        return !(this._doc != other._doc && (this._doc == null || !this._doc.equals(other._doc)));
    }

    @Override
    public int hashCode() {
        return (this._doc != null ? this._doc.hashCode() : 0);
    }

}
