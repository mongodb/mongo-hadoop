package com.mongodb.hadoop.pig;

import com.mongodb.util.ObjectSerializer;

abstract class AbstractObjectSerializer implements ObjectSerializer {

    @Override
    public String serialize(final Object obj) {
        StringBuilder builder = new StringBuilder();
        serialize(obj, builder);
        return builder.toString();
    }
}