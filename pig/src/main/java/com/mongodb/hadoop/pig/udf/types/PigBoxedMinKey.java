package com.mongodb.hadoop.pig.udf.types;

import org.bson.types.MinKey;

public class PigBoxedMinKey extends PigBoxedBSONValue<MinKey> {
    @Override
    public MinKey getObject() {
        return new MinKey();
    }
}
