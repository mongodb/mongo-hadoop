package com.mongodb.hadoop.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BSONFileSplit extends FileSplit {

    // CHECKSTYLE:OFF
    protected String keyField = "_id";
    // CHECKSTYLE:ON

    public BSONFileSplit(final Path file, final long start, final long length,
                         final String[] hosts) {
        super(file, start, length, hosts);
    }

    public BSONFileSplit() { super(); }

    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(final String keyField) {
        this.keyField = keyField;
    }
}
