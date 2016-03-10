package com.mongodb.hadoop.pig.udf;

import com.mongodb.hadoop.pig.udf.types.PigBoxedObjectId;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * Pig UDF that extracts the timestamp from an ObjectId.
 */
public class ObjectIdToSeconds extends EvalFunc<Integer> {

    public Integer exec(final Tuple input) throws IOException {
        if (null == input || input.size() == 0) {
            return null;
        }
        Object oid = input.get(0);
        if (oid instanceof PigBoxedObjectId) {
            return ((PigBoxedObjectId) oid).getObject().getTimestamp();
        } else if (oid instanceof String) {
            return new ObjectId((String) oid).getTimestamp();
        } else if (oid instanceof DataByteArray) {
            return new ObjectId(((DataByteArray) oid).get()).getTimestamp();
        }
        throw new IOException(
          "Not an ObjectId, so cannot convert to seconds: " + oid);
    }

    @Override
    public Schema outputSchema(final Schema input) {
        return new Schema(new Schema.FieldSchema("seconds", DataType.INTEGER));
    }
}
