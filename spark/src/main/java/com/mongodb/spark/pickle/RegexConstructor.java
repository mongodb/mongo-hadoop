package com.mongodb.spark.pickle;

import net.razorvine.pickle.IObjectConstructor;
import net.razorvine.pickle.PickleException;
import org.bson.BSON;

import java.util.HashMap;
import java.util.regex.Pattern;

public class RegexConstructor implements IObjectConstructor {

    public static class RegexBox extends BSONValueBox<Pattern> {
        private Pattern value;
        static {
            BSON.addEncodingHook(RegexBox.class, getTransformer());
        }

        private static int pythonFlagsToJavaFlags(final int pythonFlags) {
            int javaFlags = 0;
            if ((pythonFlags & 2) > 0) {
                javaFlags |= Pattern.CASE_INSENSITIVE;
            }
            if ((pythonFlags & 64) > 0) {
                javaFlags |= Pattern.COMMENTS;
            }
            if ((pythonFlags & 16) > 0) {
                javaFlags |= Pattern.DOTALL;
            }
            if ((pythonFlags & 8) > 0) {
                javaFlags |= Pattern.MULTILINE;
            }
            if ((pythonFlags & 32) > 0) {
                // 0x100 == Pattern.UNICODE_CHARACTER_CLASS in Java >= 7.
                javaFlags |= (Pattern.UNICODE_CASE | 0x100);
            }
            return javaFlags;
        }

        @SuppressWarnings("MagicConstant")
        // CHECKSTYLE:OFF
        public void __setstate__(final HashMap<String, Object> state) {
            // CHECKSTYLE:ON
            Object pattern = state.get("pattern");
            Object flags = state.get("flags");
            if (!((pattern instanceof String) && (flags instanceof Integer))) {
                throw new PickleException(
                  "Expected a String for key \"pattern\" and an Integer for "
                    + "key \"flags\", not a " + pattern.getClass().getName()
                    + " and a " + flags.getClass().getName());
            }
            value = Pattern.compile(
              (String) pattern, pythonFlagsToJavaFlags((Integer) flags));
        }

        @Override
        public Pattern get() {
            return value;
        }
    }

    @Override
    public Object construct(final Object[] args) {
        if (args.length != 0) {
            throw new PickleException(
              "Regex constructor requires 0 arguments, not " + args.length);
        }
        return new RegexBox();
    }
}
