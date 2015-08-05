package com.mongodb.spark.pickle;

import com.mongodb.DBRef;
import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import org.bson.BSON;

import java.util.HashMap;

public class DBRefConstructor implements IObjectConstructor {

    public static class DBRefBox extends BSONValueBox<DBRef> {
        private DBRef value;
        static {
            BSON.addEncodingHook(DBRefBox.class, getTransformer());
        }

        // CHECKSTYLE:OFF
        public void __setstate__(final HashMap<String, Object> state) {
            // CHECKSTYLE:ON
            Object collection = state.get("_DBRef__collection");
            if (!(collection instanceof String)) {
                throw new PickleException(
                  "Expected a String for key \"_DBRef__colledction\", not a "
                    + collection.getClass().getName());
            }
            this.value = new DBRef(
              (String) collection, state.get("_DBRef__id"));
        }

        @Override
        public DBRef get() {
            return value;
        }
    }

    @Override
    public Object construct(final Object[] args) {
        if (args.length != 0) {
            throw new PickleException(
              "DBRef constructor requires 0 arguments, not " + args.length);
        }
        return new DBRefBox();
    }
}
