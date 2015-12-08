package com.mongodb.hadoop.pig.udf.types;

import org.bson.types.MaxKey;

public class PigBoxedMaxKey extends PigBoxedBSONValue<MaxKey> {
    @Override
    public MaxKey getObject() {
        return new MaxKey();
    }
}
