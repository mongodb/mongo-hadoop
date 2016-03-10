package com.mongodb.hadoop.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Convenience abstract implementation of Pig's EvalFunc that automatically
 * tells Pig that the return type of the UDF is a DataByteArray.
 *
 * Subclasses specify what subclass of DataByteArray to use in the type
 * parameter T.
 */
public abstract class ByteArrayTypeEvalFunc<T> extends EvalFunc<T> {
    @Override
    public Schema outputSchema(final Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BYTEARRAY));
    }
}
