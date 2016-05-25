/*
 * Copyright 2008-2015 MongoDB, Inc.
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

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.bson.*;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.CodeWScope;
import org.bson.types.Symbol;

import java.lang.reflect.Array;
import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.BsonBinarySubType.*;

/**
 * A collectible codec for a DBObject.
 *
 * @since 3.0
 */
@SuppressWarnings("rawtypes")
public class MyDBObjectCodec implements CollectibleCodec<DBObject> {

    private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = createDefaultBsonTypeClassMap();
    private static final String ID_FIELD_NAME = "_id";

    private final CodecRegistry codecRegistry;
    private final BsonTypeClassMap bsonTypeClassMap;
    private final BasicDBObjectFactory objectFactory;
    private final IdGenerator idGenerator = new ObjectIdGenerator();

    static BsonTypeClassMap createDefaultBsonTypeClassMap() {
        Map<BsonType, Class<?>> replacements = new HashMap<BsonType, Class<?>>();
        replacements.put(BsonType.REGULAR_EXPRESSION, Pattern.class);
        replacements.put(BsonType.SYMBOL, String.class);
        replacements.put(BsonType.TIMESTAMP, BSONTimestamp.class);

        return new BsonTypeClassMap(replacements);
    }

    static BsonTypeClassMap getDefaultBsonTypeClassMap() {
        return DEFAULT_BSON_TYPE_CLASS_MAP;
    }

    /**
     * Construct an instance with the given codec registry.
     *
     * @param codecRegistry the non-null codec registry
     */
    public MyDBObjectCodec(final CodecRegistry codecRegistry) {
        this(codecRegistry, DEFAULT_BSON_TYPE_CLASS_MAP);
    }

    /**
     * Construct an instance.
     *
     * @param codecRegistry the codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     */
    public MyDBObjectCodec(final CodecRegistry codecRegistry, final BsonTypeClassMap bsonTypeClassMap) {
        this(codecRegistry, bsonTypeClassMap, new BasicDBObjectFactory());
    }

    /**
     * Construct an instance.
     *
     *  @param codecRegistry the non-null codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     * @param objectFactory the non-null object factory used to create empty DBObject instances when decoding
     */
    public MyDBObjectCodec(final CodecRegistry codecRegistry, final BsonTypeClassMap bsonTypeClassMap, final BasicDBObjectFactory objectFactory) {
        this.objectFactory = notNull("objectFactory", objectFactory);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.bsonTypeClassMap = notNull("bsonTypeClassMap", bsonTypeClassMap);
    }

    @Override
    public void encode(final BsonWriter writer, final DBObject document, final EncoderContext encoderContext) {
        writer.writeStartDocument();

        beforeFields(writer, encoderContext, document);

        for (final String key : document.keySet()) {
            if (skipField(encoderContext, key)) {
                continue;
            }
            writer.writeName(key);
            writeValue(writer, encoderContext, document.get(key));
        }
        writer.writeEndDocument();
    }

    @Override
    public DBObject decode(final BsonReader reader, final DecoderContext decoderContext) {
        List<String> path = new ArrayList<String>(10);
        return readDocument(reader, decoderContext, path);
    }

    @Override
    public Class<DBObject> getEncoderClass() {
        return DBObject.class;
    }

    @Override
    public boolean documentHasId(final DBObject document) {
        return document.containsField(ID_FIELD_NAME);
    }

    @Override
    public BsonValue getDocumentId(final DBObject document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException("The document does not contain an _id");
        }

        Object id = document.get(ID_FIELD_NAME);
        if (id instanceof BsonValue) {
            return (BsonValue) id;
        }

        BsonDocument idHoldingDocument = new BsonDocument();
        BsonWriter writer = new BsonDocumentWriter(idHoldingDocument);
        writer.writeStartDocument();
        writer.writeName(ID_FIELD_NAME);
        writeValue(writer, EncoderContext.builder().build(), id);
        writer.writeEndDocument();
        return idHoldingDocument.get(ID_FIELD_NAME);
    }

    @Override
    public DBObject generateIdIfAbsentFromDocument(final DBObject document) {
        if (!documentHasId(document)) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }
        return document;
    }

    private void beforeFields(final BsonWriter bsonWriter, final EncoderContext encoderContext, final DBObject document) {
        if (encoderContext.isEncodingCollectibleDocument() && document.containsField(ID_FIELD_NAME)) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValue(bsonWriter, null, document.get(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final EncoderContext encoderContext, final String key) {
        return encoderContext.isEncodingCollectibleDocument() && key.equals(ID_FIELD_NAME);
    }

    @SuppressWarnings("unchecked")
    private void writeValue(final BsonWriter bsonWriter, final EncoderContext encoderContext, final Object initialValue) {
        Object value = BSON.applyEncodingHooks(initialValue);
        if (value == null) {
            bsonWriter.writeNull();
        } else if (value instanceof DBRef) {
            encodeDBRef(bsonWriter, (DBRef) value);
        } else if (value instanceof Map) {
            encodeMap(bsonWriter, (Map<String, Object>) value);
        } else if (value instanceof Iterable) {
            encodeIterable(bsonWriter, (Iterable) value);
        } else if (value instanceof BSONObject) {
            encodeBsonObject(bsonWriter, ((BSONObject) value));
        } else if (value instanceof CodeWScope) {
            encodeCodeWScope(bsonWriter, (CodeWScope) value);
        } else if (value instanceof byte[]) {
            encodeByteArray(bsonWriter, (byte[]) value);
        } else if (value.getClass().isArray()) {
            encodeArray(bsonWriter, value);
        } else if (value instanceof Symbol) {
            bsonWriter.writeSymbol(((Symbol) value).getSymbol());
		} else if (value.equals(Float.NaN) || value.equals(Double.NaN)) {
			bsonWriter.writeString("NaN");
        } else {
            Codec codec = codecRegistry.get(value.getClass());
            codec.encode(bsonWriter, value, encoderContext);
        }
    }

    private void encodeMap(final BsonWriter bsonWriter, final Map<String, Object> document) {
        bsonWriter.writeStartDocument();

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            bsonWriter.writeName(entry.getKey());
            writeValue(bsonWriter, null, entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    private void encodeBsonObject(final BsonWriter bsonWriter, final BSONObject document) {
        bsonWriter.writeStartDocument();

        for (String key : document.keySet()) {
            bsonWriter.writeName(key);
            writeValue(bsonWriter, null, document.get(key));
        }
        bsonWriter.writeEndDocument();
    }

    private void encodeByteArray(final BsonWriter bsonWriter, final byte[] value) {
        bsonWriter.writeBinaryData(new BsonBinary(value));
    }

    private void encodeArray(final BsonWriter bsonWriter, final Object value) {
        bsonWriter.writeStartArray();

        int size = Array.getLength(value);
        for (int i = 0; i < size; i++) {
            writeValue(bsonWriter, null, Array.get(value, i));
        }

        bsonWriter.writeEndArray();
    }

    private void encodeDBRef(final BsonWriter bsonWriter, final DBRef dbRef) {
        bsonWriter.writeStartDocument();

        bsonWriter.writeString("$ref", dbRef.getCollectionName());
        bsonWriter.writeName("$id");
        writeValue(bsonWriter, null, dbRef.getId());

        bsonWriter.writeEndDocument();
    }

    @SuppressWarnings("unchecked")
    private void encodeCodeWScope(final BsonWriter bsonWriter, final CodeWScope value) {
        bsonWriter.writeJavaScriptWithScope(value.getCode());
        encodeBsonObject(bsonWriter, value.getScope());
    }

    private void encodeIterable(final BsonWriter bsonWriter, final Iterable iterable) {
        bsonWriter.writeStartArray();
        for (final Object cur : iterable) {
            writeValue(bsonWriter, null, cur);
        }
        bsonWriter.writeEndArray();
    }

    private Object readValue(final BsonReader reader, final DecoderContext decoderContext, final String fieldName,
                             final List<String> path) {
        Object initialRetVal;
        BsonType bsonType = reader.getCurrentBsonType();

        if (bsonType.isContainer() && fieldName != null) {
            //if we got into some new context like nested document or array
            path.add(fieldName);
        }

        switch (bsonType) {
            case DOCUMENT:
                initialRetVal = verifyForDBRef(readDocument(reader, decoderContext, path));
                break;
            case ARRAY:
                initialRetVal = readArray(reader, decoderContext, path);
                break;
            case JAVASCRIPT_WITH_SCOPE: //custom for driver-compat types
                initialRetVal = readCodeWScope(reader, decoderContext, path);
                break;
            case DB_POINTER: //custom for driver-compat types
                BsonDbPointer dbPointer = reader.readDBPointer();
                initialRetVal = new DBRef(dbPointer.getNamespace(), dbPointer.getId());
                break;
            case BINARY:
                initialRetVal = readBinary(reader, decoderContext);
                break;
            case NULL:
                reader.readNull();
                initialRetVal = null;
                break;
            default:
                initialRetVal = codecRegistry.get(bsonTypeClassMap.get(bsonType)).decode(reader, decoderContext);
        }

        if (bsonType.isContainer() && fieldName != null) {
            //step out of current context to a parent
            path.remove(fieldName);
        }

        return BSON.applyDecodingHooks(initialRetVal);
    }

    private Object readBinary(final BsonReader reader, final DecoderContext decoderContext) {
        byte bsonSubType = reader.peekBinarySubType();

        if (bsonSubType == UUID_STANDARD.getValue() || bsonSubType == UUID_LEGACY.getValue()) {
            return codecRegistry.get(UUID.class).decode(reader, decoderContext);
        } else if (bsonSubType == BINARY.getValue() || bsonSubType == OLD_BINARY.getValue()) {
            return codecRegistry.get(byte[].class).decode(reader, decoderContext);
        } else {
            return codecRegistry.get(Binary.class).decode(reader, decoderContext);
        }
    }

    private List readArray(final BsonReader reader, final DecoderContext decoderContext, final List<String> path) {
        reader.readStartArray();
        BasicDBList list = new BasicDBList();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext, null, path));
        }
        reader.readEndArray();
        return list;
    }

    private DBObject readDocument(final BsonReader reader, final DecoderContext decoderContext, final List<String> path) {
        DBObject document = objectFactory.getInstance(path);

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, decoderContext, fieldName, path));
        }

        reader.readEndDocument();
        return document;
    }

    private CodeWScope readCodeWScope(final BsonReader reader, final DecoderContext decoderContext, final List<String> path) {
        return new CodeWScope(reader.readJavaScriptWithScope(), readDocument(reader, decoderContext, path));
    }

    private Object verifyForDBRef(final DBObject document) {
        if (document.containsField("$ref") && document.containsField("$id")) {
            return new DBRef((String) document.get("$ref"), document.get("$id"));
        } else {
            return document;
        }
    }
}

