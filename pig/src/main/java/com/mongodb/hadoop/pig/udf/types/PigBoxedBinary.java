package com.mongodb.hadoop.pig.udf.types;

import org.bson.types.Binary;

public class PigBoxedBinary extends PigBoxedBSONValue<Binary> {
    public PigBoxedBinary(final byte[] b) {
        super(b);
    }

    @Override
    public Binary getObject() {
        return new Binary(get());
    }
}
