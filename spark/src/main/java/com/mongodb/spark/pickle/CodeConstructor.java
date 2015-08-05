package com.mongodb.spark.pickle;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import org.bson.BSON;
import org.bson.BasicBSONObject;
import org.bson.types.Code;
import org.bson.types.CodeWScope;

import java.util.HashMap;
import java.util.Map;

public class CodeConstructor implements IObjectConstructor {

    public static class CodeBox extends BSONValueBox<Code> {
        private String code;
        private Code value;
        static {
            BSON.addEncodingHook(CodeBox.class, getTransformer());
        }

        public CodeBox(final String code) {
            this.code = code;
        }

        // CHECKSTYLE:OFF
        public void __setstate__(final HashMap<String, Object> state) {
            // CHECKSTYLE:ON
            Object scope = state.get("_Code__scope");
            if (!(scope instanceof Map)) {
                throw new PickleException(
                  "Expected a Map for key \"_Code__scope\", not a "
                    + scope.getClass().getName());
            }
            Map scopeMap = (Map) scope;
            if (!scopeMap.isEmpty()) {
                this.value = new CodeWScope(this.code,
                  new BasicBSONObject(scopeMap));
            } else {
                this.value = new Code(this.code);
            }
        }

        @Override
        public Code get() {
            return value;
        }
    }

    @Override
    public Object construct(final Object[] args) {
        if (args.length != 1) {
            throw new PickleException(
              "Code constructor requires 1 argument, not " + args.length);
        }
        if (!(args[0] instanceof String)) {
            throw new PickleException(
              "Code constructor requries a String, not a "
              + args[0].getClass().getName());
        }
        return new CodeBox((String) args[0]);
    }
}
