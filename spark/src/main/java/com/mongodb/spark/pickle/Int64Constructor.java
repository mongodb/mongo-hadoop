package com.mongodb.spark.pickle;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import org.bson.BSON;

import java.util.HashMap;

public class Int64Constructor implements IObjectConstructor {

    public static class Int64Box extends BSONValueBox<Long> {
        private Long value;
        static {
            BSON.addEncodingHook(Int64Box.class, getTransformer());
        }

        public Int64Box(final Long value) {
            this.value = value;
        }

        // CHECKSTYLE:OFF
        public void __setstate__(HashMap<String, Object> state) {
            // No state to set.
        }
        // CHECKSTYLE:ON

        @Override
        public Long get() {
            return this.value;
        }
    }

    @Override
    public Object construct(final Object[] args) {
        if (args.length != 1) {
            throw new PickleException(
              "Int64 constructor requires 1 argument, not " + args.length);
        }
        if (!((args[0] instanceof Integer) || (args[0] instanceof Long))) {
            throw new PickleException(
              "Int64 constructor requires an Integer or Long, not a "
                + args[0].getClass().getName());
        }
        return new Int64Box((Long) args[0]);
    }
}
