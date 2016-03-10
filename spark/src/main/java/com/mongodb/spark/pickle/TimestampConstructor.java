package com.mongodb.spark.pickle;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import org.bson.BSON;
import org.bson.types.BSONTimestamp;

import java.util.HashMap;

public class TimestampConstructor implements IObjectConstructor {

    public static class TimestampBox extends BSONValueBox<BSONTimestamp> {
        private BSONTimestamp value;
        static {
            BSON.addEncodingHook(TimestampBox.class, getTransformer());
        }

        // CHECKSTYLE:OFF
        public void __setstate__(final HashMap state) {
            // CHECKSTYLE:ON
            Object time = state.get("_Timestamp__time");
            Object inc = state.get("_Timestamp__inc");
            if (!((time instanceof Integer) && (inc instanceof Integer))) {
                throw new PickleException(
                  "Excpected Integer for keys \"_Timestamp__time\" and "
                    + "\"Timestamp__inc\", not a "
                    + time.getClass().getName() + " and a "
                    + inc.getClass().getName());
            }
            value = new BSONTimestamp((Integer) time, (Integer) inc);
        }

        public BSONTimestamp get() {
            return value;
        }
    }

    @Override
    public Object construct(final Object[] args) {
        if (args.length != 0) {
            throw new PickleException(
              "Timestamp constructor requires 0 arguments, not " + args.length);
        }
        return new TimestampBox();
    }
}
