package com.mongodb.hadoop.pig.udf;

import com.mongodb.hadoop.pig.udf.types.PigBoxedObjectId;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * UDF that transforms the incoming value into a BSON ObjectId.
 */
public class ToObjectId extends ByteArrayTypeEvalFunc<PigBoxedObjectId> {
    public PigBoxedObjectId exec(final Tuple input) throws IOException {
        if (null == input || input.size() == 0) {
            return null;
        }
        Object o = input.get(0);
        if (o instanceof String) {
            return new PigBoxedObjectId(
              new ObjectId((String) o).toByteArray());
        } else if (o instanceof DataByteArray) {
            return new PigBoxedObjectId(((DataByteArray) o).get());
        }
        throw new IOException(
          "Need a String or DataByteArray to build an ObjectId, not " + o);
    }
}
