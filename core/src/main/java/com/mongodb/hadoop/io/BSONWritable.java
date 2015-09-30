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

@SuppressWarnings("deprecation")
public class BSONWritable implements WritableComparable {

    private static final byte[] HEX_CHAR = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private static final Log LOG = LogFactory.getLog(BSONWritable.class);

    static {
        WritableComparator.define(BSONWritable.class, new BSONWritableComparator());
    }

    private BSONObject doc;

    public BSONWritable() {
        doc = new BasicBSONObject();
    }

    public BSONWritable(final BSONObject doc) {
        this();
        setDoc(doc);
    }

    public void setDoc(final BSONObject doc) {
        this.doc = doc;
    }

    public BSONObject getDoc() {
        return doc;
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
        enc.putObject(doc);
        enc.done();
        buf.pipe(new DataOutputOutputStreamAdapter(out));
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
            doc = (BSONObject) cb.get();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Decoded a BSON Object: " + doc);
            }
        } catch (Exception e) {
            /* If we can't read another length it's not an error, just return quietly. */
            // TODO - Figure out how to gracefully mark this as an empty
            LOG.info("No Length Header available." + e);
            doc = new BasicDBObject();
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "<BSONWritable:" + doc + ">";
    }

    /**
     * Used by child copy constructors.
     * @param other the Writable to copy.
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
        return new BSONWritableComparator().compare(this, o);
    }

    protected static void dumpBytes(final BasicOutputBuffer buf) {// Temp debug output
        dumpBytes(buf.toByteArray());
    }

    protected static void dumpBytes(final byte[] buffer) {
        StringBuilder sb = new StringBuilder(2 + 3 * buffer.length);

        for (byte b : buffer) {
            sb.append("0x").append((char) HEX_CHAR[(b & 0x00F0) >> 4]).append((char) HEX_CHAR[b & 0x000F]).append(" ");
        }

        LOG.info("Byte Dump: " + sb);
    }

    /**
     * Unwrap a (usually Writable) Object, getting back a value suitable for
     * putting into a BSONObject. If the given object is not Writable, then
     * simply return the Object back.
     *
     * @param x the Object to turn into BSON.
     * @return the BSON representation of the Object.
     */
    @SuppressWarnings("unchecked")
    public static Object toBSON(final Object x) {
        if (x == null) {
            return null;
        }
        if (x instanceof Text) {
            return x.toString();
        }
        if (x instanceof BSONWritable) {
            return ((BSONWritable) x).getDoc();
        }
        if (x instanceof Writable) {
            if (x instanceof AbstractMapWritable) {
                if (!(x instanceof Map)) {
                    throw new IllegalArgumentException(
                      String.format("Cannot turn %s into BSON, since it does "
                                      + "not implement java.util.Map.",
                                    x.getClass().getName()));
                }
                Map<Writable, Writable> map = (Map<Writable, Writable>) x;
                BasicBSONObject bson = new BasicBSONObject();
                for (Map.Entry<Writable, Writable> entry : map.entrySet()) {
                    bson.put(entry.getKey().toString(),
                             toBSON(entry.getValue()));
                }
                return bson;
            }
            if (x instanceof ArrayWritable) {
                Writable[] o = ((ArrayWritable) x).get();
                Object[] a = new Object[o.length];
                for (int i = 0; i < o.length; i++) {
                    a[i] = toBSON(o[i]);
                }
                return a;
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
        return x;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final BSONWritable other = (BSONWritable) obj;
        return !(doc != other.doc && (doc == null || !doc.equals(other.doc)));
    }

    @Override
    public int hashCode() {
        return doc != null ? doc.hashCode() : 0;
    }

}
