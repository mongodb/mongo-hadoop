package com.mongodb.spark.pickle;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import org.bson.BSON;
import org.bson.types.MinKey;

import java.util.HashMap;

public class MinKeyConstructor implements IObjectConstructor {

    public static class MinKeyBox extends BSONValueBox<MinKey> {
        private static final MinKey MIN_KEY = new MinKey();
        static {
            BSON.addEncodingHook(MinKeyBox.class, getTransformer());
        }

        // CHECKSTYLE:OFF
        public void __setstate__(final HashMap<String, Object> state) {
            // no state to set here.
        }
        // CHECKSTYLE:ON

        @Override
        public MinKey get() {
            return MIN_KEY;
        }
    }

    @Override
    public Object construct(final Object[] args) {
        if (args.length != 0) {
            throw new PickleException(
              "MinKey constructor requires 0 arguments, not " + args.length);
        }
        return new MinKeyBox();
    }
}
