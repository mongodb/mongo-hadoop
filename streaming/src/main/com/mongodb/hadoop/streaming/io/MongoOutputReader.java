package com.mongodb.hadoop.streaming.io;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.OutputReader;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

public class MongoOutputReader extends OutputReader<ObjectWritable, BSONWritable> {

    @Override
    public void initialize(PipeMapRed pipeMapRed) throws IOException {
        super.initialize(pipeMapRed);
        in = pipeMapRed.getClientInput();
    }

    @Override
    public boolean readKeyValue() throws IOException {
        // Actually, just read the value as the key is embedded.
        bson = new BSONWritable();
        bson.readFields( in );
        // If successful we'll have an _id field
        return bson.containsKey("_id");
    }

    @Override
    public ObjectWritable getCurrentKey() throws IOException {
        return new ObjectWritable(bson.get("_id"));
    }

    @Override
    public BSONWritable getCurrentValue() throws IOException {
        return bson;
    }

    @Override
    public String getLastOutput() {
        return bson.toString();
    }

    private DataInput in;
    private BSONWritable bson;
}
