package com.mongodb.hadoop.pig.udf.types;

import com.mongodb.DBRef;
import org.bson.types.ObjectId;

import java.util.Arrays;

public class PigBoxedDBRef extends PigBoxedBSONValue<DBRef> {
    public PigBoxedDBRef(final byte[] b) {
        super(b);
    }

    @Override
    public DBRef getObject() {
        byte[] bytes = get();
        ObjectId id = new ObjectId(
          Arrays.copyOfRange(bytes, bytes.length - 12, bytes.length));
        String collectionName = new String(
          Arrays.copyOfRange(bytes, 0, bytes.length - 13));

        return new DBRef(collectionName, id);
    }
}
