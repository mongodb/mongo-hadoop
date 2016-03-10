package com.mongodb.hadoop.pig.udf;

import com.mongodb.hadoop.pig.udf.types.PigBoxedMaxKey;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Pig UDF that always returns MaxKey().
 */
public class GenMaxKey extends ByteArrayTypeEvalFunc<PigBoxedMaxKey> {
    @Override
    public PigBoxedMaxKey exec(final Tuple input) throws IOException {
        return new PigBoxedMaxKey();
    }
}
