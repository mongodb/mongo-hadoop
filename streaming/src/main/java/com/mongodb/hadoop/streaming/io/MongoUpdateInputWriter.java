package com.mongodb.hadoop.streaming.io;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.InputWriter;

import java.io.DataOutput;
import java.io.IOException;

/**
 * InputWriter capable of handling both BSONWritable and MongoUpdateWritable
 * as value types.
 */
public class MongoUpdateInputWriter extends InputWriter<Object, Writable> {

    private DataOutput output;
    private final BSONWritable bsonWritable = new BSONWritable();

    @Override
    public void initialize(final PipeMapRed pipeMapRed) throws IOException {
        super.initialize(pipeMapRed);
        output = pipeMapRed.getClientOutput();
    }

    @Override
    public void writeKey(final Object key) throws IOException {
        // Nothing to do.
    }

    @Override
    public void writeValue(final Writable value) throws IOException {
        if (value instanceof MongoUpdateWritable) {
            // If we're writing to the input of a streaming script, just send
            // back the "query" portion of the MongoUpdateWritable, so that
            // mapper and reducer scripts can operate on a single document.
            bsonWritable.setDoc(((MongoUpdateWritable) value).getQuery());
            bsonWritable.write(output);
        } else if (value instanceof BSONWritable) {
            value.write(output);
        } else {
            throw new IOException("Unexpected Writable type :" + value);
        }
    }
}
