package com.mongodb.hadoop.mapred.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BSONFileSplit extends FileSplit {

    // CHECKSTYLE:OFF
    protected String keyField = "_id";
    // CHECKSTYLE:ON


    public BSONFileSplit(final Path file, final long start, final long
      length, final String[] hosts) {
        super(file, start, length, hosts);
    }

    public BSONFileSplit() { this(null, 0, 0, null); }

    public String getKeyField() { return keyField; }

    public void setKeyField(final String keyField) {
        this.keyField = keyField;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, getKeyField());
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        super.readFields(in);
        setKeyField(Text.readString(in));
    }
}
