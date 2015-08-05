package com.mongodb.spark.pickle;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import org.bson.BSON;
import org.bson.types.MaxKey;

import java.util.HashMap;

public class MaxKeyConstructor implements IObjectConstructor {

    public static class MaxKeyBox extends BSONValueBox<MaxKey> {
        private static final MaxKey MAX_KEY = new MaxKey();
        static {
            BSON.addEncodingHook(MaxKeyBox.class, getTransformer());
        }

        // CHECKSTYLE:OFF
        public void __setstate__(final HashMap<String, Object> state) {
            // no state to set here.
        }
        // CHECKSTYLE:ON

        @Override
        public MaxKey get() {
            return MAX_KEY;
        }
    }

    @Override
    public Object construct(final Object[] args) {
        if (args.length != 0) {
            throw new PickleException(
              "MaxKey constructor requires 0 arguments, not " + args.length);
        }
        return new MaxKeyBox();
    }
}
