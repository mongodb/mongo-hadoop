/*
 * Copyright 2010-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.hive;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static java.lang.String.format;

/**
 * The BSONSerDe class deserializes (parses) and serializes object from BSON to Hive represented object. It's initialized with the hive
 * columns and hive recognized types as well as other config variables mandated by the StorageHanders.
 */
public class BSONSerDe implements SerDe {
    private static final Log LOG = LogFactory.getLog(BSONSerDe.class);

    // stores the 1-to-1 mapping of MongoDB fields to hive columns
    public static final String MONGO_COLS = "mongo.columns.mapping";

    // ObjectId should be translated to a struct, these are
    // the pre-defined field names and values identifying
    // that struct as an ObjectId struct
    private static final int BSON_NUM = 8;
    private static final String OID = "oid";
    private static final String BSON_TYPE = "bsontype";

    private StructTypeInfo docTypeInfo;
    private ObjectInspector docOI;
    //CHECKSTYLE:OFF
    public List<String> columnNames;
    public List<TypeInfo> columnTypes;

    // maps hive columns to fields in a MongoDB collection
    public Map<String, String> hiveToMongo;
    //CHECKSTYLE:ON

    // A row represents a row in the Hive table 
    private List<Object> row = new ArrayList<Object>();

    // BSONWritable to hold documents to be serialized.
    private BSONWritable bsonWritable;

    /**
     * Finds out the information of the table, including the column names and types.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void initialize(final Configuration conf, final Properties tblProps) throws SerDeException {
        // regex used to split column names between commas
        String splitCols = "\\s*,\\s*";

        // Get the table column names
        String colNamesStr = tblProps.getProperty(serdeConstants.LIST_COLUMNS);
        columnNames = Arrays.asList(colNamesStr.split(splitCols));

        // Get mappings specified by the user
        if (tblProps.containsKey(MONGO_COLS)) {
            String mongoFieldsStr = tblProps.getProperty(MONGO_COLS);
            Map<String, String> rules = ((BasicBSONObject) JSON.parse(mongoFieldsStr)).toMap();

            // register the hive field mappings to mongo field mappings
            hiveToMongo = new HashMap<String, String>();
            registerMappings(rules);
        }

        // Get the table column types
        String colTypesStr = tblProps.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);

        if (columnNames.size() != columnTypes.size()) {
            throw new SerDeException("Column Names and Types don't match in size");
        }

        // Get the structure and object inspector
        docTypeInfo =
            (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        docOI =
            TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(docTypeInfo);

        // Create the BSONWritable instance for future use.
        bsonWritable = new BSONWritable();
    }


    /**
     * Takes in the object represented by JSON for Hive to Mongo/BSON mapping. Records these mappings and infers upper level mappings from
     * lower level declarations.
     */
    private void registerMappings(final Map<String, String> rules) throws SerDeException {
        // explode/infer shorter mappings
        for (Entry e : rules.entrySet()) {
            String key = (String) e.getKey();
            String value = (String) e.getValue();

            if (hiveToMongo.containsKey(key) && !hiveToMongo.get(key).equals(value)) {
                throw new SerDeException("Ambiguous rule definition for " + key);
            } else {
                hiveToMongo.put(key.toLowerCase(), value);
            }

            if (key.contains(".")) {
                // split by "."
                String[] miniKeys = key.split("\\.");
                String[] miniValues = value.split("\\.");

                if (miniKeys.length != miniValues.length) {
                    throw new SerDeException(key + " should be of same depth as " + value);
                }

                int i = 0;
                String curKey = "", curValue = "";
                while (i < miniKeys.length - 1) {
                    curKey += miniKeys[i];
                    curValue += miniValues[i];

                    if (hiveToMongo.containsKey(curKey) && !hiveToMongo.get(curKey).equals(curValue)) {
                        throw new SerDeException("Ambiguous rule definition for " + curKey);
                    } else {
                        hiveToMongo.put(curKey.toLowerCase(), curValue);
                    }

                    curKey += ".";
                    curValue += ".";
                    i += 1;
                }
            }
        }
    }


    /**
     * Given a Writable object of BSON, turn it into a Hive table row
     */
    @Override
    //CHECKSTYLE:OFF
    public Object deserialize(final Writable writable) throws SerDeException {
        //CHECKSTYLE:ON
        BSONObject doc;
        row.clear();

        // Make sure it's a BSONWritable object
        if (writable instanceof BSONWritable) {
            doc = ((BSONWritable) writable).getDoc();
        } else {
            throw new SerDeException(format("%srequires a BSONWritable object, not%s", getClass(), writable.getClass()));
        }

        // For each field, cast it to a HIVE type and add to the current row
        Object value;
        List<String> structFieldNames = docTypeInfo.getAllStructFieldNames();
        for (String fieldName : structFieldNames) {
            try {
                TypeInfo fieldTypeInfo = docTypeInfo.getStructFieldTypeInfo(fieldName);

                // get the corresponding field name in MongoDB
                String mongoMapping;
                if (hiveToMongo == null) {
                    mongoMapping = fieldName;
                } else {
                    mongoMapping = hiveToMongo.containsKey(fieldName)
                                   ? hiveToMongo.get(fieldName)
                                   : fieldName;
                }
                value = deserializeField(getValue(doc, mongoMapping), fieldTypeInfo, fieldName);
            } catch (Exception e) {
                LOG.warn("Could not find the appropriate field for name " + fieldName);
                value = null;
            }
            row.add(value);
        }

        return row;
    }

    private Object getValue(final BSONObject doc, final String mongoMapping) {
        if (mongoMapping.contains(".")) {
            int index = mongoMapping.indexOf('.');
            BSONObject object = (BSONObject) doc.get(mongoMapping.substring(0, index));
            return getValue(object, mongoMapping.substring(index + 1));
        }
        return doc.get(mongoMapping);
    }


    /**
     * Get the Hive representation for a value given its {@code TypeInfo}.
     * @param value the value for which to get the Hive representation
     * @param valueTypeInfo a description of the value's type
     * @param ext the field name
     * @return the Hive representation of the value
     */
    public Object deserializeField(final Object value, final TypeInfo valueTypeInfo, final String ext) {
        if (value != null) {
            switch (valueTypeInfo.getCategory()) {
                case LIST:
                    return deserializeList(value, (ListTypeInfo) valueTypeInfo, ext);
                case MAP:
                    return deserializeMap(value, (MapTypeInfo) valueTypeInfo, ext);
                case PRIMITIVE:
                    return deserializePrimitive(value, (PrimitiveTypeInfo) valueTypeInfo);
                case STRUCT:
                    // Supports both struct and map, but should use struct 
                    return deserializeStruct(value, (StructTypeInfo) valueTypeInfo, ext);
                case UNION:
                    // Mongo also has no union
                    LOG.warn("BSONSerDe does not support unions.");
                    return null;
                default:
                    // Must be an unknown (a Mongo specific type)
                    return deserializeMongoType(value);
            }
        }
        return null;
    }


    /**
     * Deserialize a List with the same listElemTypeInfo for its elements
     * @param value the value for which to get the Hive representation
     * @param valueTypeInfo a description of the value's type
     * @param ext the field name
     * @return the Hive representation of the value
     */
    private Object deserializeList(final Object value, final ListTypeInfo valueTypeInfo, final String ext) {
        BasicBSONList list = (BasicBSONList) value;
        TypeInfo listElemTypeInfo = valueTypeInfo.getListElementTypeInfo();

        for (int i = 0; i < list.size(); i++) {
            list.set(i, deserializeField(list.get(i), listElemTypeInfo, ext));
        }
        return list.toArray();
    }


    /**
     * deserialize the struct stored in 'value' ext : the hive mapping(s) seen so far before 'value' is encountered.
     * @param value the value for which to get the Hive representation
     * @param valueTypeInfo a description of the value's type
     * @param ext the field name
     * @return the Hive representation of the value
     */
    @SuppressWarnings("unchecked")
    private Object deserializeStruct(final Object value, final StructTypeInfo valueTypeInfo, final String ext) {
        // ObjectId will be stored in a special struct
        if (value instanceof ObjectId) {
            return deserializeObjectId(value, valueTypeInfo);
        } else {
            Map<Object, Object> map = (Map<Object, Object>) value;

            ArrayList<String> structNames = valueTypeInfo.getAllStructFieldNames();
            ArrayList<TypeInfo> structTypes = valueTypeInfo.getAllStructFieldTypeInfos();

            List<Object> struct = new ArrayList<Object>(structNames.size());

            for (int i = 0; i < structNames.size(); i++) {
                String fieldName = structNames.get(i);

                // hiveMapping -> prefixed by parent struct names. 
                // For example, in {"wife":{"name":{"first":"Sydney"}}},
                // the hiveMapping of "first" is "wife.name.first"
                String hiveMapping = ext.length() == 0 ? fieldName : ext + "." + fieldName;

                // get the corresponding field name in MongoDB
                String mongoMapping;
                if (hiveToMongo == null) {
                    mongoMapping = hiveMapping;
                } else {
                    if (hiveToMongo.containsKey(hiveMapping)) {
                        mongoMapping = hiveToMongo.get(hiveMapping);
                    } else {
                        mongoMapping = ext.length() > 0 && hiveToMongo.containsKey(ext)
                                       ? hiveToMongo.get(ext) + "." + fieldName
                                       : hiveMapping;
                    }
                }

                String nextFieldTrans = extractMongoField(mongoMapping, hiveMapping, ext);
                struct.add(deserializeField(map.get(nextFieldTrans), structTypes.get(i), hiveMapping));
            }
            return struct;
        }
    }


    /*
     * Gets the next field to be extracted in the process of (recursively) mapping fields in
     * MongoDB to Hive struct field names
     */
    private String extractMongoField(final String mongoMapping, final String hiveMapping, final String ext) {
        String[] splitMongo = mongoMapping.split("\\.");
        String[] splitHive = hiveMapping.split("\\.");

        int i = 0;
        String mongoSeen = "", hiveSeen = "";
        while (i < splitMongo.length - 1) {
            mongoSeen += splitMongo[i];
            hiveSeen += splitHive[i];

            if (hiveSeen.equals(ext)) {
                return splitMongo[i + 1];
            }

            mongoSeen += ".";
            hiveSeen += ".";
            i++;
        }

        return null;
    }


    /**
     * Also deserialize a Map with the same mapElemTypeInfo
     * @param value the value for which to get the Hive representation
     * @param valueTypeInfo a description of the value's type
     * @param ext the field name
     * @return the Hive representation of the value
     */
    private Object deserializeMap(final Object value, final MapTypeInfo valueTypeInfo, final String ext) {
        BasicBSONObject b = (BasicBSONObject) value;
        TypeInfo mapValueTypeInfo = valueTypeInfo.getMapValueTypeInfo();

        for (Entry<String, Object> entry : b.entrySet()) {
            b.put(entry.getKey(), deserializeField(entry.getValue(), mapValueTypeInfo, ext));
        }

        return b.toMap();
    }


    /**
     * Most primitives are included, but some are specific to Mongo instances
     * @param value the value for which to get the Hive representation
     * @param valueTypeInfo a description of the value's type
     * @return the Hive representation of the value
     */
    private Object deserializePrimitive(final Object value, final PrimitiveTypeInfo valueTypeInfo) {
        switch (valueTypeInfo.getPrimitiveCategory()) {
            case BINARY:
                return value;
            case BOOLEAN:
                return value;
            case DOUBLE:
                return ((Number) value).doubleValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case INT:
                return ((Number) value).intValue();
            case LONG:
                return ((Number) value).longValue();
            case SHORT:
                return ((Number) value).shortValue();
            case STRING:
                return value.toString();
            case TIMESTAMP:
                if (value instanceof Date) {
                    return new Timestamp(((Date) value).getTime());
                } else if (value instanceof BSONTimestamp) {
                    return new Timestamp(((BSONTimestamp) value).getTime() * 1000L);
                } else {
                    return value;
                }
            default:
                return deserializeMongoType(value);
        }
    }


    /**
     * For Mongo Specific types, return the most appropriate java types
     * @param value the value for which to get the Hive representation
     * @return the Hive representation of the value
     */
    private Object deserializeMongoType(final Object value) {
        if (value instanceof Symbol) {
            return value.toString();
        } else {

            LOG.error("Unable to parse " + value + " for type " + value.getClass());
            return null;
        }
    }


    /**
     * Parses an ObjectId into the corresponding struct declared in Hive
     * @param value the value for which to get the Hive representation
     * @param valueTypeInfo a description of the value's type
     * @return the Hive representation of the value
     */
    private Object deserializeObjectId(final Object value, final StructTypeInfo valueTypeInfo) {
        ArrayList<String> structNames = valueTypeInfo.getAllStructFieldNames();

        List<Object> struct = new ArrayList<Object>(structNames.size());
        for (String structName : structNames) {
            LOG.warn("SWEET ------ structName is " + structName);
            if (structName.equals(OID)) {
                struct.add(value.toString());
            } else if (structName.equals(BSON_TYPE)) {
                // the bson type is an int order type
                // http://docs.mongodb.org.manual/faq/developers/
                struct.add(BSON_NUM);
            }
        }
        return struct;
    }


    @Override
    //CHECKSTYLE:OFF
    public ObjectInspector getObjectInspector() throws SerDeException {
        //CHECKSTYLE:ON
        return docOI;
    }


    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }


    @Override
    public Class<? extends Writable> getSerializedClass() {
        return BSONWritable.class;
    }


    //CHECKSTYLE:OFF
    @Override
    public Writable serialize(final Object obj, final ObjectInspector oi) throws SerDeException {
        bsonWritable.setDoc(
          (BSONObject) serializeStruct(obj, (StructObjectInspector) oi, ""));
        return bsonWritable;
    }
    //CHECKSTYLE:ON


    public Object serializeObject(final Object obj, final ObjectInspector oi, final String ext) {
        switch (oi.getCategory()) {
            case LIST:
                return serializeList(obj, (ListObjectInspector) oi, ext);
            case MAP:
                return serializeMap(obj, (MapObjectInspector) oi, ext);
            case PRIMITIVE:
                return serializePrimitive(obj, (PrimitiveObjectInspector) oi);
            case STRUCT:
                return serializeStruct(obj, (StructObjectInspector) oi, ext);
            case UNION:
            default:
                LOG.error("Cannot serialize " + obj + " of type " + obj);
                break;
        }
        return null;
    }


    private Object serializeList(final Object obj, final ListObjectInspector oi, final String ext) {
        BasicBSONList list = new BasicBSONList();
        List<?> field = oi.getList(obj);

        if (field == null) {
            return list;
        }

        ObjectInspector elemOI = oi.getListElementObjectInspector();

        for (Object elem : field) {
            list.add(serializeObject(elem, elemOI, ext));
        }

        return list;
    }


    /**
     * Turn a Hive struct into a BasicBSONObject.
     * @param obj the Hive struct
     * @param structOI an {@code ObjectInspector} for the struct
     * @param ext the field name
     * @return a BasicBSONObject representing the Hive struct
     */
    private Object serializeStruct(final Object obj, final StructObjectInspector structOI, final String ext) {
        if (ext.length() > 0 && isObjectIdStruct(obj, structOI)) {

            String objectIdString = "";
            for (StructField s : structOI.getAllStructFieldRefs()) {
                if (s.getFieldName().equals(OID)) {
                    objectIdString = structOI.getStructFieldData(obj, s).toString();
                    break;
                }
            }
            return new ObjectId(objectIdString);
        } else {

            BasicBSONObject bsonObject = new BasicBSONObject();
            // fields is the list of all variable names and information within the struct obj
            List<? extends StructField> fields = structOI.getAllStructFieldRefs();

            for (int i = 0; i < fields.size(); i++) {
                StructField field = fields.get(i);

                String fieldName, hiveMapping;

                // get corresponding mongoDB field  
                if (ext.length() == 0) {
                    fieldName = columnNames.get(i);
                    hiveMapping = fieldName;
                } else {
                    fieldName = field.getFieldName();
                    hiveMapping = ext + "." + fieldName;
                }

                ObjectInspector fieldOI = field.getFieldObjectInspector();
                Object fieldObj = structOI.getStructFieldData(obj, field);

                if (hiveToMongo != null && hiveToMongo.containsKey(hiveMapping)) {
                    String mongoMapping = hiveToMongo.get(hiveMapping);
                    int lastDotPos = mongoMapping.lastIndexOf(".");
                    String lastMapping = lastDotPos == -1 ? mongoMapping : mongoMapping.substring(lastDotPos + 1);
                    bsonObject.put(lastMapping,
                                   serializeObject(fieldObj, fieldOI, hiveMapping));
                } else {
                    bsonObject.put(fieldName,
                                   serializeObject(fieldObj, fieldOI, hiveMapping));
                }
            }

            return bsonObject;
        }
    }

    /**
     * Determine whether a Hive struct should be serialized as an ObjectId.
     * @param obj the Hive struct
     * @param structOI an {@code ObjectInspector} for the struct
     * @return {@code true} if the struct should be interpreted as an ObjectId
     */
    private boolean isObjectIdStruct(final Object obj, final StructObjectInspector structOI) {
        List<? extends StructField> fields = structOI.getAllStructFieldRefs();

        // If the struct are of incorrect size, then there's no need to create
        // a list of names
        if (fields.size() != 2) {
            return false;
        }
        boolean hasOID = false;
        boolean isBSONType = false;
        for (StructField s : fields) {
            String fieldName = s.getFieldName();
            if (fieldName.equals(OID)) {
                hasOID = true;
            } else if (fieldName.equals(BSON_TYPE)) {
                String num = structOI.getStructFieldData(obj, s).toString();
                isBSONType = Integer.parseInt(num) == BSON_NUM;
            }

        }
        return hasOID && isBSONType;
    }


    /**
     * Serialize a Hive Map into a BSONObject.
     * @param obj the Hive Map.
     * @param mapOI an {@code ObjectInspector} for the Hive Map.
     * @param ext the field name
     * @return a BSONObject representing the Hive Map
     */
    private Object serializeMap(final Object obj, final MapObjectInspector mapOI, final String ext) {
        BasicBSONObject bsonObject = new BasicBSONObject();
        ObjectInspector mapValOI = mapOI.getMapValueObjectInspector();

        // Each value is guaranteed to be of the same type
        for (Entry<?, ?> entry : mapOI.getMap(obj).entrySet()) {
            String field = entry.getKey().toString();
            Object value = serializeObject(entry.getValue(), mapValOI, ext);
            bsonObject.put(field, value);
        }
        return bsonObject;
    }

    /**
     * Serialize a Hive primitive type into a BSON type.
     * @param obj the primitive Hive object
     * @param oi an {@code ObjectInspector} for the Hive primitive
     * @return a BSON primitive object
     */
    private Object serializePrimitive(final Object obj, final PrimitiveObjectInspector oi) {
        switch (oi.getPrimitiveCategory()) {
            case TIMESTAMP:
                Timestamp ts = (Timestamp) oi.getPrimitiveJavaObject(obj);
                if (ts == null) {
                    return null;
                }
                return new Date(ts.getTime());
            default:
                return oi.getPrimitiveJavaObject(obj);
        }
    }
}
