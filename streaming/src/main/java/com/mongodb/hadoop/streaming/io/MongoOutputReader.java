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
    private BSONWritable bson;
    private static final Log LOG = LogFactory.getLog(MongoOutputReader.class);

    @Override
    public void initialize(final PipeMapRed pipeMapRed) throws IOException {
        super.initialize(pipeMapRed);
        in = pipeMapRed.getClientInput();
    }

    @Override
    public boolean readKeyValue() throws IOException {
        // Actually, just read the value as the key is embedded.
        try {
            bson = new BSONWritable();
            bson.readFields(in);
            // If successful we'll have an _id field
            return bson.getDoc().containsField("_id");
        } catch (IndexOutOfBoundsException e) {
            // No more data
            LOG.info("No more data; no key/value pair read.");
            return false;
        }
    }

    @Override
    public BSONWritable getCurrentKey() throws IOException {
        return new BSONWritable(new BasicDBObject("_id", bson.getDoc().get("_id")));
    }

    @Override
    public BSONWritable getCurrentValue() throws IOException {
        return bson;
    }

    @Override
    public String getLastOutput() {
        return bson.toString();
    }
}
