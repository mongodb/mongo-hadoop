package com.mongodb.hadoop.streaming.io;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.InputWriter;

import java.io.DataOutput;
import java.io.IOException;

public class MongoInputWriter extends InputWriter<Object, BSONWritable> {

    private DataOutput out;

    @Override
    public void initialize(final PipeMapRed pipeMapRed) throws IOException {
        super.initialize(pipeMapRed);
        out = pipeMapRed.getClientOutput();
    }

    @Override
    public void writeKey(final Object key) throws IOException {
        // We skip the key COMPLETELY as it's just a copy of _id
        // and readable by the BSON implementation
    }

    @Override
    public void writeValue(final BSONWritable value) throws IOException {
        value.write(out);
    }
}
