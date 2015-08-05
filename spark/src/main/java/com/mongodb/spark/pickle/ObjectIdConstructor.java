package com.mongodb.spark.pickle;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import org.bson.BSON;
import org.bson.types.ObjectId;

public class ObjectIdConstructor implements IObjectConstructor {

    public static class ObjectIdBox extends BSONValueBox<ObjectId> {
        private ObjectId oid;
        static {
            BSON.addEncodingHook(ObjectIdBox.class, getTransformer());
        }

        // CHECKSTYLE:OFF
        public void __setstate__(final String state) {
            // CHECKSTYLE:ON
            byte[] oidBytes = new byte[state.length()];
            for (int i = 0; i < state.length(); ++i) {
                oidBytes[i] = (byte) state.charAt(i);
            }
            this.oid = new ObjectId(oidBytes);
        }

        @Override
        public ObjectId get() {
            return this.oid;
        }
    }

    @Override
    public Object construct(final Object[] args) {
        if (args.length != 0) {
            throw new PickleException(
              "ObjectId constructor requires 0 arguments, not " + args.length);
        }
        return new ObjectIdBox();
    }
}
