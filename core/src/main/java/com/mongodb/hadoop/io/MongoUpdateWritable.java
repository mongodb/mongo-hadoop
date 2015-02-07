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
import org.apache.hadoop.io.Writable;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is <em>not</em> reusable.
 */

@SuppressWarnings("deprecation")
public class MongoUpdateWritable implements Writable {

    private static final Log LOG = LogFactory.getLog(MongoUpdateWritable.class);
    private BasicBSONObject query;
    private BasicBSONObject modifiers;
    private boolean upsert;

    private boolean multiUpdate;
    private BSONEncoder enc = new BasicBSONEncoder();

    private BasicOutputBuffer buf = new BasicOutputBuffer();

    public MongoUpdateWritable(final BasicBSONObject query, final BasicBSONObject modifiers, final boolean upsert,
                               final boolean multiUpdate) {
        this.query = query;
        this.modifiers = modifiers;
        this.upsert = upsert;
        this.multiUpdate = multiUpdate;
    }

    public MongoUpdateWritable(final BasicBSONObject query, final BasicBSONObject modifiers) {
        this(query, modifiers, true, false);
    }

    public BasicBSONObject getQuery() {
        return query;
    }

    public BasicBSONObject getModifiers() {
        return modifiers;
    }

    public boolean isUpsert() {
        return upsert;
    }


    public boolean isMultiUpdate() {
        return multiUpdate;
    }

    /**
     * /** {@inheritDoc}
     *
     * @see Writable#write(DataOutput)
     */
    public void write(final DataOutput out) throws IOException {
        enc.set(buf);
        enc.putObject(query);
        enc.done();
        enc.set(buf);
        enc.putObject(modifiers);
        enc.done();
        buf.pipe(out);
        out.writeBoolean(upsert);
        out.writeBoolean(multiUpdate);
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
            byte[] data = new byte[dataLen + 4];
            System.arraycopy(l, 0, data, 0, 4);
            in.readFully(data, 4, dataLen - 4);
            dec.decode(data, cb);
            query = (BasicBSONObject) cb.get();
            in.readFully(l);
            dataLen = Bits.readInt(l);
            data = new byte[dataLen + 4];
            System.arraycopy(l, 0, data, 0, 4);
            in.readFully(data, 4, dataLen - 4);
            dec.decode(data, cb);
            modifiers = (BasicBSONObject) cb.get();
            upsert = in.readBoolean();
            multiUpdate = in.readBoolean();
        } catch (Exception e) {
            /* If we can't read another length it's not an error, just return quietly. */
            // TODO - Figure out how to gracefully mark this as an empty
            LOG.info("No Length Header available." + e);
            query = new BasicDBObject();
            modifiers = new BasicDBObject();
        }

    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final MongoUpdateWritable other = (MongoUpdateWritable) obj;
        if (upsert != other.upsert || multiUpdate != other.multiUpdate) {
            return false;
        }
        if ((query == null && other.query != null)
                || (other.query == null && query != null)
                || (!query.equals(other.query))) {
            return false;
        }
        if ((modifiers == null && other.modifiers != null)
                || (other.modifiers == null && modifiers != null)
                || (!modifiers.equals(other.modifiers))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = query.hashCode();
        hashCode ^= modifiers.hashCode();
        hashCode ^= (upsert ? 1 : 0) << 1;
        hashCode ^= (multiUpdate ? 1 : 0) << 2;
        return hashCode;
    }
}
