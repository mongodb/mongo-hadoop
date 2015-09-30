package com.mongodb.hadoop.streaming.io;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.OutputReader;

import java.io.DataInput;
import java.io.IOException;

public class MongoOutputReader extends OutputReader<BSONWritable, BSONWritable> {

    private DataInput in;
    private static final Log LOG = LogFactory.getLog(MongoOutputReader.class);
    private BSONWritable currentKey;
    private BSONWritable currentValue;

    @Override
    public void initialize(final PipeMapRed pipeMapRed) throws IOException {
        super.initialize(pipeMapRed);
        in = pipeMapRed.getClientInput();
        this.currentKey = new BSONWritable();
        this.currentValue = new BSONWritable();
    }

    @Override
    public boolean readKeyValue() throws IOException {
        // Actually, just read the value as the key is embedded.
        try {
            currentValue.readFields(in);
            Object id = currentValue.getDoc().get("_id");
            currentKey.setDoc(new BasicDBObject("_id", id));
            // If successful we'll have an _id field
            return id != null;
        } catch (IndexOutOfBoundsException e) {
            // No more data
            LOG.info("No more data; no key/value pair read.");
            return false;
        }
    }

    @Override
    public BSONWritable getCurrentKey() throws IOException {
        return currentKey;
    }

    @Override
    public BSONWritable getCurrentValue() throws IOException {
        return currentValue;
    }

    @Override
    public String getLastOutput() {
        return currentValue.toString();
    }
}
