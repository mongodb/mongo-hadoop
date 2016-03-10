package com.mongodb.hadoop.pig.udf;

import com.mongodb.hadoop.pig.udf.types.PigBoxedBinary;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Pig UDF that transforms the incoming value into a BSON Binary object.
 */
public class ToBinary extends ByteArrayTypeEvalFunc<PigBoxedBinary> {
    @Override
    public PigBoxedBinary exec(final Tuple input) throws IOException {
        if (null == input || input.size() == 0) {
            return null;
        }
        Object o = input.get(0);
        if (o instanceof String) {
            return new PigBoxedBinary(((String) o).getBytes());
        } else if (o instanceof DataByteArray) {
            return new PigBoxedBinary(((DataByteArray) o).get());
        }
        throw new IOException(
          "Need String or DataByteArray to build a Binary, not " + o);
    }
}
