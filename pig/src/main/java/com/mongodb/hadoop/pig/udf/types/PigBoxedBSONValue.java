package com.mongodb.hadoop.pig.udf.types;

import org.apache.pig.data.DataByteArray;

public abstract class PigBoxedBSONValue<T> extends DataByteArray {
    public PigBoxedBSONValue() {}

    public PigBoxedBSONValue(final byte[] b) {
        super(b);
    }

    public abstract T getObject();
}
