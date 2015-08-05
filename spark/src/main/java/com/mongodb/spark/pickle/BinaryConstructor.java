package com.mongodb.spark.pickle;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import org.bson.BSON;
import org.bson.types.Binary;

import java.util.HashMap;

public class BinaryConstructor implements IObjectConstructor {

    public static class BinaryBox extends BSONValueBox<Binary> {
        private Binary value = null;
        static {
            BSON.addEncodingHook(BinaryBox.class, getTransformer());
        }

        public BinaryBox(final String data, final int type) {
            byte[] byteData = new byte[data.length()];
            for (int i = 0; i < byteData.length; ++i) {
                byteData[i] = (byte) data.charAt(i);
            }
            this.value = new Binary((byte) type, byteData);
        }

        // CHECKSTYLE:OFF
        public void __setstate__(final HashMap<String, Object> hm) {
            // State has already been set from constructor.
        }
        // CHECKSTYLE:ON

        @Override
        public Binary get() {
            return value;
        }
    }

    @Override
    public Object construct(final Object[] args) {
        if (args.length != 2) {
            throw new PickleException(
              "Binary constructor requires 2 arguments, not " + args.length);
        }
        if (!((args[0] instanceof String) && (args[1] instanceof Integer))) {
            throw new PickleException(
              "Binary constructor takes a String and an Integer, "
                + "not a " + args[0].getClass().getName()
                + " and a " + args[1].getClass().getName());
        }
        return new BinaryBox((String) args[0], (Integer) args[1]);
    }
}
