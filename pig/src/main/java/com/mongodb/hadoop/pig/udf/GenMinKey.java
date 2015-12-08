package com.mongodb.hadoop.pig.udf;

import com.mongodb.hadoop.pig.udf.types.PigBoxedMinKey;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Pig UDF that always returns MinKey().
 */
public class GenMinKey extends ByteArrayTypeEvalFunc<PigBoxedMinKey> {
    @Override
    public PigBoxedMinKey exec(final Tuple input) throws IOException {
        return new PigBoxedMinKey();
    }
}
