package com.mongodb.hadoop.pig.udf;

import com.mongodb.hadoop.pig.udf.types.PigBoxedDBRef;
import org.apache.pig.data.Tuple;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.Map;

/**
 * Pig UDF that transforms the incoming value into a MongoDB DBRef.
 */
public class ToDBRef extends ByteArrayTypeEvalFunc<PigBoxedDBRef> {
    @Override
    public PigBoxedDBRef exec(final Tuple input) throws IOException {
        if (null == input || input.size() == 0) {
            return null;
        }
        Object o = input.get(0);
        if (o instanceof Map) {
            Object collectionName = ((Map) o).get("$ref");
            Object id = ((Map) o).get("$id");
            if (null == collectionName || null == id) {
                throw new IOException(
                  "Map must contain both $ref and $id fields: " + o);
            }
            byte[] collectionNameBytes =
              ((String) collectionName).getBytes();
            byte[] dbrefBytes =
              new byte[12 + 1 + collectionNameBytes.length];
            byte[] oidBytes = new ObjectId((String) id).toByteArray();
            System.arraycopy(
              collectionNameBytes, 0,
              dbrefBytes, 0, collectionNameBytes.length);
            dbrefBytes[collectionNameBytes.length] = 0;
            System.arraycopy(
              oidBytes, 0,
              dbrefBytes, collectionNameBytes.length + 1, 12);
            return new PigBoxedDBRef(dbrefBytes);
        }
        throw new IOException("Need a Map to build a DBRef, not " + o);
    }
}
