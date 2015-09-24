package com.mongodb.hadoop.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

class DataOutputOutputStreamAdapter extends OutputStream {
    private final DataOutput dataOutput;

    DataOutputOutputStreamAdapter(final DataOutput dataOutput) {
        this.dataOutput = dataOutput;
    }

    @Override
    public void write(final int b) throws IOException {
        dataOutput.write(b);
    }

    @Override
    public void write(final byte[] b) throws IOException {
        dataOutput.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        dataOutput.write(b, off, len);
    }
}
