package com.mongodb.hadoop.streaming.io;

import org.bson.BSONObject;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.InputWriter;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MongoInputWriter extends InputWriter<Object, BSONWritable> {

    @Override
    public void initialize( PipeMapRed pipeMapRed ) throws IOException{
        super.initialize( pipeMapRed );
        out = pipeMapRed.getClientOutput();
    }

    @Override
    public void writeKey( Object key ) throws IOException{
        // We skip the key COMPLETELY as it's just a copy of _id
        // and readable by the BSON implementation
    }

    @Override
    public void writeValue( BSONWritable value ) throws IOException{
        //TODO this could be more efficient
        value.write(out);
    }

    private DataOutput out;
    private static final Log log = LogFactory.getLog(MongoInputWriter.class);
}
