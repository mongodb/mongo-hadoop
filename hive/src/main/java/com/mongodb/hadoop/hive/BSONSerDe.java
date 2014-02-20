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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.Writable;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.*;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.util.JSON;

/**
 * The BSONSerDe class deserializes (parses) and serializes object from
 * BSON to Hive represented object. It's initialized with the hive
 * columns and hive recognized types as well as other config variables
 * mandated by the StorageHanders. 
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
    public List<String> columnNames;
    public List<TypeInfo> columnTypes;
    
    // maps hive columns to fields in a MongoDB collection
    public Map<String, String> hiveToMongo;    
    
    // A row represents a row in the Hive table 
    private List<Object> row = new ArrayList<Object>();
    
    /**
     * Finds out the information of the table, including the column names and types. 
     */
    @SuppressWarnings("unchecked")
    @Override
    public void initialize(Configuration conf, Properties tblProps)
        throws SerDeException {
        // regex used to split column names between commas
        String splitCols = "\\s*,\\s*";
        
        // Get the table column names
        String colNamesStr = tblProps.getProperty(serdeConstants.LIST_COLUMNS);
        this.columnNames = Arrays.asList(colNamesStr.split(splitCols));

        // Get mappings specified by the user
        if (tblProps.containsKey(this.MONGO_COLS)) {
            String mongoFieldsStr = tblProps.getProperty(this.MONGO_COLS);
            Map<String, String> rules = ((BasicBSONObject) JSON.parse(mongoFieldsStr)).toMap();

            // register the hive field mappings to mongo field mappings
            this.hiveToMongo = new HashMap<String, String>();
            registerMappings(rules);
        }
        
        // Get the table column types
        String colTypesStr = tblProps.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        this.columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        
        if (this.columnNames.size() != this.columnTypes.size()) {
            throw new SerDeException("Column Names and Types don't match in size");
        }
        
        // Get the structure and object inspector
        this.docTypeInfo = 
            (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(this.columnNames, this.columnTypes);
        this.docOI = 
            TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(this.docTypeInfo);
    }
    

    /**
     * Takes in the object represented by JSON for Hive to Mongo/BSON mapping.
     * Records these mappings and infers upper level mappings from lower level
     * declarations.
     */
    private void registerMappings(Map<String, String> rules) throws SerDeException {
        // explode/infer shorter mappings
        for (Entry e : rules.entrySet()) {
            String key = (String) e.getKey();
            String value = (String) e.getValue(); 

            if (this.hiveToMongo.containsKey(key) && !this.hiveToMongo.get(key).equals(value)) {
                throw new SerDeException("Ambiguous rule definition for " + key); 
            } else {
                this.hiveToMongo.put(key, value);
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
                while ( i < miniKeys.length-1 ) {
                    curKey += miniKeys[i];
                    curValue += miniValues[i];                    

                    if (this.hiveToMongo.containsKey(curKey) && !this.hiveToMongo.get(curKey).equals(curValue)) {
                        throw new SerDeException("Ambiguous rule definition for " + curKey);
                    } else {
                        this.hiveToMongo.put(curKey, curValue);
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
    public Object deserialize(Writable writ) throws SerDeException {
        BSONObject doc = null;
        this.row.clear();
        
        // Make sure it's a BSONWritable object
        if (writ instanceof BSONWritable) {
            doc = ((BSONWritable) writ).getDoc();
        } else {
            throw new SerDeException(getClass().toString() + 
                                     "requires a BSONWritable object, not" + writ.getClass());
        }
        
        // For each field, cast it to a HIVE type and add to the current row
        Object value = null;
        List<String> structFieldNames = this.docTypeInfo.getAllStructFieldNames();
        for (int i = 0; i < structFieldNames.size(); i++) {
            String fieldName = structFieldNames.get(i);
            try {
                TypeInfo fieldTypeInfo = this.docTypeInfo.getStructFieldTypeInfo(fieldName);
                
                // get the corresponding field name in MongoDB
                String mongoMapping;
                if (this.hiveToMongo == null) {
                    mongoMapping = fieldName;
                } else {
                    mongoMapping = this.hiveToMongo.containsKey(fieldName) ?
                                        this.hiveToMongo.get(fieldName) : 
                                        fieldName;
                }
                value = deserializeField(doc.get(mongoMapping), fieldTypeInfo, fieldName); 
            } catch (Exception e) {
                LOG.warn("Could not find the appropriate field for name " + fieldName);
                value = null;
            }
            this.row.add(value);
        }
        
        return this.row;
    }
    

    /**
     * For a given Object value and its supposed TypeInfo
     * determine and return its Hive object representation
     * 
     * Map in here must be of the same type, so instead an embedded doc
     * becomes a struct instead. ***
     * 
     */
    public Object deserializeField(Object value, TypeInfo valueTypeInfo, String ext) {
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
     */
    private Object deserializeList(Object value, ListTypeInfo valueTypeInfo, String ext) {
        BasicBSONList list = (BasicBSONList) value;
        TypeInfo listElemTypeInfo = valueTypeInfo.getListElementTypeInfo();
        
        for (int i = 0 ; i < list.size() ; i++) {
            list.set(i, deserializeField(list.get(i), listElemTypeInfo, ext));
        }
        return list.toArray();
    }
    

    /**
     * deserialize the struct stored in 'value' 
     * ext : the hive mapping(s) seen so far before 'value' is encountered.
     */        
    @SuppressWarnings("unchecked")
    private Object deserializeStruct(Object value, StructTypeInfo valueTypeInfo, String ext) {
        // ObjectId will be stored in a special struct
        if (value instanceof ObjectId) {
            return deserializeObjectId(value, valueTypeInfo);
        } else {
            Map<Object, Object> map = (Map<Object, Object>) value;
            
            ArrayList<String> structNames = valueTypeInfo.getAllStructFieldNames();
            ArrayList<TypeInfo> structTypes = valueTypeInfo.getAllStructFieldTypeInfos();
                        
            List<Object> struct = new ArrayList<Object> (structNames.size());
            
            for (int i = 0 ; i < structNames.size() ; i++) {
                String fieldName = structNames.get(i);
                
                // hiveMapping -> prefixed by parent struct names. 
                // For example, in {"wife":{"name":{"first":"Sydney"}}},
                // the hiveMapping of "first" is "wife.name.first"
                String hiveMapping = ext.length() == 0 ? fieldName : (ext + "." + fieldName);
                
                // get the corresponding field name in MongoDB
                String mongoMapping;
                if (this.hiveToMongo == null) {
                    mongoMapping = hiveMapping;
                } else {
                    if (this.hiveToMongo.containsKey(hiveMapping)) {
                        mongoMapping = this.hiveToMongo.get(hiveMapping);
                    } else {
                        mongoMapping = ext.length() > 0 && this.hiveToMongo.containsKey(ext) ? 
                                          (this.hiveToMongo.get(ext)+"."+fieldName) : 
                                          hiveMapping;
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
    private String extractMongoField(String mongoMapping, String hiveMapping, String ext) {
       String[] splitMongo = mongoMapping.split("\\.");
       String[] splitHive = hiveMapping.split("\\.");

       int i = 0;
       String mongoSeen = "", hiveSeen = "";
       while (i < splitMongo.length-1) {
           mongoSeen += splitMongo[i];
           hiveSeen += splitHive[i];
           
           if ( hiveSeen.equals(ext) ) {
               return splitMongo[i+1];
           }
           
           mongoSeen += ".";
           hiveSeen += ".";           
           i++;                   
       }
       
       return null;
    }     
    

    /**
     * Also deserialize a Map with the same mapElemTypeInfo
     */
    private Object deserializeMap(Object value, MapTypeInfo valueTypeInfo, String ext) {
        BasicBSONObject b = (BasicBSONObject) value;
        TypeInfo mapValueTypeInfo = valueTypeInfo.getMapValueTypeInfo();
        
        for (Entry<String, Object> entry : b.entrySet()) {
            b.put(entry.getKey(), deserializeField(entry.getValue(), mapValueTypeInfo, ext));
        }
    
        return b.toMap();
    }    
    

    /**
     * Most primitives are included, but some are specific to Mongo instances
     */
    private Object deserializePrimitive(Object value, PrimitiveTypeInfo valueTypeInfo) {
        switch (valueTypeInfo.getPrimitiveCategory()) {
            case BINARY:
                return (byte[]) value;
            case BOOLEAN:
                return (Boolean) value;
            case DOUBLE:
                return (Double) value;
            case FLOAT:
                return (Float) value;
            case INT:
                if (value instanceof Double) {
                    return ((Double) value).intValue(); 
                }
                return (Integer) value;
            case LONG:
                return (Long) value;
            case SHORT:
                return (Short) value;
            case STRING:
                return value.toString();
            case TIMESTAMP:
                if (value instanceof Date) {
                    return new Timestamp(((Date) value).getTime());
                } else if (value instanceof BSONTimestamp) {
                    return new Timestamp(((BSONTimestamp) value).getTime() * 1000L);
                } else {
                    return (Timestamp) value;
                }
            default:
                return deserializeMongoType(value);
        }
    }
    
    
    /**
     * 
     * For Mongo Specific types, return the most appropriate java types
     */
    private Object deserializeMongoType(Object value) {
        if (value instanceof Symbol) {
            return ((Symbol) value).toString();
        } else {
        
            LOG.error("Unable to parse " + value.toString() + " for type " + 
                       value.getClass().toString());
            return null;
        }
    }
    

    /**
     * Parses an ObjectId into the corresponding struct declared in Hive
     */
    private Object deserializeObjectId(Object value, StructTypeInfo valueTypeInfo) {
        ArrayList<String> structNames = valueTypeInfo.getAllStructFieldNames();
    
        List<Object> struct = new ArrayList<Object> (structNames.size());
        for (int i = 0 ; i < structNames.size() ; i++) {
            LOG.warn("SWEET ------ structName is " + structNames.get(i));
            if (structNames.get(i).equals(this.OID)) {
                struct.add(((ObjectId) value).toString());
            } else if (structNames.get(i).equals(this.BSON_TYPE)) {
                // the bson type is an int order type
                // http://docs.mongodb.org.manual/faq/developers/
                struct.add(this.BSON_NUM);
            }
        }
        return struct;
    }
    

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return this.docOI;
    }
    

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    } 
    

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return BSONWritable.class;
    }


    @Override
    public Writable serialize(Object obj, ObjectInspector oi)
        throws SerDeException {
        return new BSONWritable((BSONObject) serializeStruct(obj, 
                    (StructObjectInspector) oi, ""));
    }
    

    public Object serializeObject(Object obj, ObjectInspector oi, String ext) {
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
                LOG.error("Cannot serialize " + obj.toString() + " of type " + obj.toString());
                break;
        }
        return null;
    }
    

    private Object serializeList(Object obj, ListObjectInspector oi, String ext) {
        BasicBSONList list = new BasicBSONList();
        List<?> field = oi.getList(obj);
        ObjectInspector elemOI = oi.getListElementObjectInspector();
    
        for (Object elem : field) {
            list.add(serializeObject(elem, elemOI, ext));
        }
    
        return list;
    }
    

    /**
     * Turn struct obj into a BasicBSONObject
     */
    private Object serializeStruct(Object obj, 
            StructObjectInspector structOI, 
            String ext) {
        if (ext.length() > 0 && isObjectIdStruct(obj, structOI)) {
            
            String objectIdString = "";
            for (StructField s : structOI.getAllStructFieldRefs()) {
                if (s.getFieldName().equals(this.OID)) {
                    objectIdString = structOI.getStructFieldData(obj, s).toString();
                    break;
                }
            }
            return new ObjectId(objectIdString);
        } else {
        
            BasicBSONObject bsonObject = new BasicBSONObject();
            // fields is the list of all variable names and information within the struct obj
            List<? extends StructField> fields = structOI.getAllStructFieldRefs();
        
            for (int i = 0 ; i < fields.size() ; i++) {
                StructField field = fields.get(i);
        
                String fieldName, hiveMapping;
                
                // get corresponding mongoDB field  
                if (ext.length() == 0) {
                    fieldName = this.columnNames.get(i);
                    hiveMapping = fieldName;
                } else {
                    fieldName = field.getFieldName();
                    hiveMapping = (ext + "." + fieldName);
                }
                
                ObjectInspector fieldOI = field.getFieldObjectInspector();
                Object fieldObj = structOI.getStructFieldData(obj, field);
                
                if (this.hiveToMongo != null && this.hiveToMongo.containsKey(hiveMapping)) {
                    String mongoMapping = this.hiveToMongo.get(hiveMapping);
                    int lastDotPos = mongoMapping.lastIndexOf(".");
                    String lastMapping = lastDotPos == -1 ? mongoMapping :  mongoMapping.substring(lastDotPos+1);
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
     *
     * Given a struct, look to se if it contains the fields that a ObjectId
     * struct should contain
     */
    private boolean isObjectIdStruct(Object obj, StructObjectInspector structOI) {
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
            if (fieldName.equals(this.OID)) {
                hasOID = true;
            } else if (fieldName.equals(this.BSON_TYPE)) {
                String num = structOI.getStructFieldData(obj, s).toString();
                isBSONType = (Integer.parseInt(num) == this.BSON_NUM);
            }

        }
        return hasOID && isBSONType;
    }  
    

    /**
     * For a map of <String, Object> convert to an embedded document 
     */
    private Object serializeMap(Object obj, MapObjectInspector mapOI, String ext) {
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
     * For primitive types, depending on the primitive type, 
     * cast it to types that Mongo supports
     */
    private Object serializePrimitive(Object obj, PrimitiveObjectInspector oi) {
        switch (oi.getPrimitiveCategory()) {
            case BOOLEAN:
                return (Boolean) obj;
            case BINARY:
            case BYTE:
                return (byte[]) obj;
            case DOUBLE:
            case FLOAT:
                return (Double) obj;
            case LONG:
            case SHORT:
            case INT:
                if (obj instanceof LazyInteger) {
                    return Integer.parseInt(((LazyInteger) obj).toString());
                }
                return (Integer) obj;
            case STRING:
                if (obj instanceof LazyString) {
                    return ((LazyString) obj).toString();
                } else if (obj instanceof String) {
                    return (String) obj;
                }
                return obj.toString();
            case TIMESTAMP:
                return new BSONTimestamp(((Long) (((Timestamp) obj).getTime() / 1000L)).intValue(), 1);
            case UNKNOWN:
            case VOID:
            default:
                return null;
        }
    }
}
