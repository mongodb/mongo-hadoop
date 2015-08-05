package com.mongodb.spark.pickle;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.Writable;
import org.bson.BasicBSONObject;
import org.bson.Transformer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Base class for containers that hold BSON values.
 * These containers are used when unpickling objects from Python. Generally,
 * these objects implement a "__setstate__" method that allows their internal
 * state to be set after they are created.
 *
 * @param <T> the type of BSON value to be held.
 */
abstract class BSONValueBox<T> implements Writable, Serializable {

    private static final Transformer TRANSFORMER = new Transformer() {
        @Override
        public Object transform(final Object objectToTransform) {
            if (!(objectToTransform instanceof BSONValueBox)) {
                throw new IllegalArgumentException(
                  "Can only transform instances of BSONValueBox, not "
                    + objectToTransform);
            }
            return ((BSONValueBox) objectToTransform).get();
        }
    };

    public abstract T get();

    static Transformer getTransformer() {
        return TRANSFORMER;
    }

    /**
     * Inflate a BSONValueBox from a DataInput.
     * This method is here so that BSONValueBox implements Hadoop's Writable
     * interface, which is a requirement to use this type with Spark Hadoop
     * RDDs. However, you should never call this method directly.
     *
     * @param in the DataInput.
     * @throws IOException is always thrown when this method is called.
     */
    @Override
    public void readFields(final DataInput in) throws IOException {
        throw new IOException("Cannot read fields into a BSONValueBox.");
    }

    /**
     * Write a BSONValueBox type to a DataOutput.
     * This method is here so that BSONValueBox implements Hadoop's Writable
     * interface, which is a requirement to use this type with Spark's Hadoop
     * RDDs. Calling this method will write into the output a document of the
     * form:
     * <code>
     *     {"value": (boxed value)}
     * </code>
     * @param out the DataOutput
     * @throws IOException when there is an error writing to the DataOutput
     */
    @Override
    public void write(final DataOutput out) throws IOException {
        (new BSONWritable(new BasicBSONObject("value", get()))).write(out);
    }
}
