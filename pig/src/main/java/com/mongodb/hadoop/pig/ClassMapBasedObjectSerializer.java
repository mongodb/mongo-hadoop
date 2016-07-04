/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.pig;

import com.mongodb.Bytes;
import com.mongodb.util.ObjectSerializer;
import org.bson.util.ClassMap;

import java.util.List;


/**
 * <p>Objects of type ClassMapBasedObjectSerializer are constructed to perform instance specific object to JSON serialization schemes.</p>
 *
 * <p>This class is not thread safe</p>
 *
 * @author breinero
 */
public class ClassMapBasedObjectSerializer extends AbstractObjectSerializer {

    /**
     * Assign a ObjectSerializer to perform a type specific serialization scheme
     *
     * @param c          this object's type serves as a key in the serialization map. ClassMapBasedObjectSerializer uses
     *                   org.bson.util.ClassMap and not only checks if 'c' is a key in the Map, but also walks the up superclass and
     *                   interface graph of 'c' to find matches. This means that it is only necessary assign ObjectSerializers to base
     *                   classes. @see org.bson.util.ClassMap
     * @param serializer performs the serialization mapping specific to the @param key type
     */
    @SuppressWarnings("rawtypes")
    void addObjectSerializer(final Class c, final ObjectSerializer serializer) {
        _serializers.put(c, serializer);
    }

    /**
     * @param obj the object to be serialized
     * @param buf StringBuilder containing the JSON representation of the object
     */
    @Override
    public void serialize(final Object obj, final StringBuilder buf) {
        Object objectToSerialize = obj;

        objectToSerialize = Bytes.applyEncodingHooks(objectToSerialize);

        if (objectToSerialize == null) {
            buf.append(" null ");
            return;
        }

        ObjectSerializer serializer = null;

        List<Class<?>> ancestors;
        ancestors = ClassMap.getAncestry(objectToSerialize.getClass());

        for (final Class<?> ancestor : ancestors) {
            serializer = _serializers.get(ancestor);
            if (serializer != null) {
                break;
            }
        }

        if (serializer == null && objectToSerialize.getClass().isArray()) {
            serializer = _serializers.get(Object[].class);
        }

        if (serializer == null) {
            throw new RuntimeException("json can't serialize type : " + objectToSerialize.getClass());
        }

        serializer.serialize(objectToSerialize, buf);
    }

    private final ClassMap<ObjectSerializer> _serializers = new ClassMap<ObjectSerializer>();
}