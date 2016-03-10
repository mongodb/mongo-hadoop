package com.mongodb.hadoop.pig.udf.types;

import org.bson.types.ObjectId;

public class PigBoxedObjectId extends PigBoxedBSONValue<ObjectId> {
    public PigBoxedObjectId(final byte[] b) {
        super(b);
    }

    @Override
    public ObjectId getObject() {
        return new ObjectId(get());
    }
}
