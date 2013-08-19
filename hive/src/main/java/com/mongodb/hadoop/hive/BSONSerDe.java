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

public class BSONSerDe implements SerDe {
    private static final Log LOG = LogFactory.getLog(BSONSerDe.class);
    
    // ObjectId should be translated to a struct, these are
    // the pre-defined field names and values identifying
    // that struct as an ObjectId struct
    private static final int BSON_TYPE = 8;
    private static final String OID = "oid";
    
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
        columnNames = Arrays.asList(colNamesStr.split(splitCols));
        
        // Get mappings specified by the user
        if (tblProps.containsKey(MongoStorageHandler.MONGO_COLS)) {
            String mongoFieldsStr = tblProps.getProperty(MongoStorageHandler.MONGO_COLS);
            Map<String, String> rules = ((BasicBSONObject) JSON.parse(mongoFieldsStr)).toMap();
            
            hiveToMongo = new HashMap<String, String>();
            
            // explode/infer shorter mappings
            for (Entry e : rules.entrySet()) {
                String key = (String) e.getKey();
                String value = (String) e.getValue(); 
                
                if ( hiveToMongo.containsKey(key) && !hiveToMongo.get(key).equals(value)) {
                    throw new SerDeException("Ambiguous rule definition for " + key);                    
                } else {
                    hiveToMongo.put(key, value);
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
                        
                        if ( hiveToMongo.containsKey(curKey) && !hiveToMongo.get(curKey).equals(curValue)) {
                            throw new SerDeException("Ambiguous rule definition for " + curKey);
                        } else {
                            hiveToMongo.put(curKey, curValue);
                        }
                        
                        curKey += ".";
                        curValue += ".";
                        
                        i += 1;
                    }                    
                }                
            }
        }
        
        // Get the table column types
        String colTypesStr = tblProps.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        
        assert( columnNames.size() == columnTypes.size()) :
        "Column Names and Types don't match in size";
        
        // Get the structure and object inspector
        docTypeInfo = 
            (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        docOI = 
            TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(docTypeInfo);
    }
    
    /**
     * Given a Writable object of BSON, turn it into a Hive table row
     */
    @Override
    public Object deserialize(Writable writ) throws SerDeException {
        BSONObject doc = null;
        row.clear();
        
        // Make sure it's a BSONWritable object
        if (writ instanceof BSONWritable) {
            doc = ((BSONWritable) writ).getDoc();
        } else {
            throw new SerDeException(getClass().toString() + 
                                     "requires a BSONWritable object, not" + writ.getClass());
        }
        
        // For each field, cast it to a HIVE type and add to the current row
        Object value = null;
        List<String> structFieldNames = docTypeInfo.getAllStructFieldNames();
        for (int i = 0; i < structFieldNames.size(); i++) {
            String fieldName = structFieldNames.get(i);
            try {
                TypeInfo fieldTypeInfo = docTypeInfo.getStructFieldTypeInfo(fieldName);
                
                // get the corresponding field name in MongoDB
                String fieldTrans;
                if (hiveToMongo == null) {
                    fieldTrans = fieldName;
                } else {
                    fieldTrans = hiveToMongo.containsKey(fieldName) ? hiveToMongo.get(fieldName) : fieldName;
                }
                
                value = deserializeField(doc, doc.get(fieldTrans), fieldTypeInfo, fieldName);                  
            } catch (Exception e) {
                value = null;
            }
            row.add(value);
        }
        
        return row;
    }
    
    /**
     * For a given Object value and its supposed TypeInfo
     * determine and return its Hive object representation
     * 
     * Map in here must be of the same type, so instead an embedded doc
     * becomes a struct instead. ***
     * 
     */
    public Object deserializeField(Object whole, Object value, TypeInfo valueTypeInfo, String ext) {
        if (value != null) {
            switch (valueTypeInfo.getCategory()) {
                case LIST:
                    return deserializeList(whole, value, (ListTypeInfo) valueTypeInfo, ext);
                case MAP:
                    return deserializeMap(whole, value, (MapTypeInfo) valueTypeInfo, ext);
                case PRIMITIVE:
                    return deserializePrimitive(value, (PrimitiveTypeInfo) valueTypeInfo);
                case STRUCT:
                    // Supports both struct and map, but should use struct 
                    return deserializeStruct(whole, value, (StructTypeInfo) valueTypeInfo, ext);
                case UNION:
                    // Mongo also has no union
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
    private Object deserializeList(Object whole, Object value, ListTypeInfo valueTypeInfo, String ext) {
        BasicBSONList list = (BasicBSONList) value;
        TypeInfo listElemTypeInfo = valueTypeInfo.getListElementTypeInfo();
    
        for (int i = 0 ; i < list.size() ; i++) {
            list.set(i, deserializeField(whole, list.get(i), listElemTypeInfo, ext));
        }
        return list.toArray();
    }
    
    /**
     * 
     * @param value
     * @param valueTypeInfo
     * @return
     */        
    @SuppressWarnings("unchecked")
    private Object deserializeStruct(Object whole, Object value, StructTypeInfo valueTypeInfo, String ext) {    
        if (value instanceof ObjectId) {
            return deserializeObjectId(value, valueTypeInfo);
        } else {
            Map<Object, Object> map = (Map<Object, Object>) value;
            
            ArrayList<String> structNames = valueTypeInfo.getAllStructFieldNames();
            ArrayList<TypeInfo> structTypes = valueTypeInfo.getAllStructFieldTypeInfos();
                        
            List<Object> struct = new ArrayList<Object> (structNames.size());
            
            for (int i = 0 ; i < structNames.size() ; i++) {
                String fieldName = structNames.get(i);
                
                // fullFieldName -> prefixed by parent struct names. 
                // For example, in {"wife":{"name":{"first":"Sydney"}}},
                // the fullFieldName of "first" is "wife.name.first"
                String fullFieldName = ext.length() == 0 ? fieldName : (ext + "." + structNames.get(i));
                
                // get the corresponding field name in MongoDB
                String fieldTrans;
                if (hiveToMongo == null) {
                    fieldTrans = fullFieldName;
                } else {
                    if (hiveToMongo.containsKey(fullFieldName)) {
                        fieldTrans = hiveToMongo.get(fullFieldName);
                    } else {
                        fieldTrans = ext.length() > 0 && hiveToMongo.containsKey(ext) ? 
                                              (hiveToMongo.get(ext)+"."+fieldName) : 
                                              fullFieldName;
                    }
                }
                    
                // traverse the document 'whole' and return the value of 'fieldTrans'
                Object in = traverseDocument(whole, fieldTrans);

                struct.add(deserializeField(whole, in, structTypes.get(i), fullFieldName));
            }
            return struct;
        }
    }
    
    /*
     * traverse the MongoDB document, looking for the field 'fieldTrans'
     */
    private Object traverseDocument(Object o, String fieldTrans) {
        BSONObject b = (BSONObject) o;
        int dotPos = fieldTrans.indexOf(".");
        
        if (dotPos == -1) {
            return b.get(fieldTrans);
        } else {
            String first = fieldTrans.substring(0, dotPos);
            Object inner = b.get(first);
            
            return traverseDocument(inner, fieldTrans.substring(dotPos+1));
        }
    }
    
    /**
     * Also deserialize a Map with the same mapElemTypeInfo
     */
    private Object deserializeMap(Object whole, Object value, MapTypeInfo valueTypeInfo, String ext) {
        BasicBSONObject b = (BasicBSONObject) value;
        TypeInfo mapValueTypeInfo = valueTypeInfo.getMapValueTypeInfo();
        
        for (Entry<String, Object> entry : b.entrySet()) {
            b.put(entry.getKey(), deserializeField(whole, entry.getValue(), mapValueTypeInfo, ext));
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
        // TODO:: Add more here
        if (value instanceof ObjectId) {
            return ((ObjectId) value).toString();
        } else if (value instanceof Symbol) {
            return ((Symbol) value).toString();
        } else {
        
            LOG.error("Unable to parse " + value.toString() + " for type " + 
                    value.getClass().toString());
            return null;
        }
    }
    
    private Object deserializeObjectId(Object value, StructTypeInfo valueTypeInfo) {
        ArrayList<String> structNames = valueTypeInfo.getAllStructFieldNames();
    
        List<Object> struct = new ArrayList<Object> (structNames.size());
        for (int i = 0 ; i < structNames.size() ; i++) {
            if (structNames.get(i).equals(OID)) {
                struct.add(((ObjectId) value).toString());
            } else if (structNames.get(i).equals("bsonType")) {
                // the bson type is an int order type
                // http://docs.mongodb.org.manual/faq/developers/
                struct.add(BSON_TYPE);
            }
        }
        return struct;
    }
    
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return docOI;
    }
    
    @Override
    public SerDeStats getSerDeStats() {
        //TODO:: this needs to be determined. what is it?
        return null;
    } 
    
    @Override
    public Class<? extends Writable> getSerializedClass() {
        return BSONWritable.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector oi)
            throws SerDeException {
        return new BSONWritable((BSONObject) serializeStruct(obj, (StructObjectInspector) oi, ""));
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
        if (ext.length() > 0 && isObjectIdStruct(structOI)) {
            
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
        
            for (int i = 0 ; i < fields.size() ; i++) {
                StructField field = fields.get(i);
        
                String fieldName, fullFieldName;
                
                // get corresponding mongoDB field  
                if (ext.length() == 0) {
                    fieldName = columnNames.get(i);
                    fullFieldName = fieldName;
                } else {
                    fieldName = field.getFieldName();
                    fullFieldName = (ext + "." + fieldName);
                }
                
                ObjectInspector fieldOI = field.getFieldObjectInspector();
                Object fieldObj = structOI.getStructFieldData(obj, field);
                
                if (hiveToMongo != null && hiveToMongo.containsKey(fullFieldName)) {
                    bsonObject.put(getLastPart(hiveToMongo.get(fullFieldName)), 
                                   serializeObject(fieldObj, fieldOI, fullFieldName));
                } else {
                    bsonObject.put(fieldName, 
                                   serializeObject(fieldObj, fieldOI, fullFieldName));   
                }
            }
            
            return bsonObject;
        }
    }
    
    /*
     * Returns the part of the String, after the last '.'
     */
    String getLastPart(String s) {
        int lastDotPos = s.lastIndexOf(".");
        if (lastDotPos == -1) {
            return s;
        } else {
            return s.substring(lastDotPos+1);
        }
    }
    
    /**
     *
     * Given a struct, look to se if it contains the fields that a ObjectId
     * struct should contain
     */
    private boolean isObjectIdStruct(StructObjectInspector structOI) {
        List<? extends StructField> fields = structOI.getAllStructFieldRefs();
    
        // If the struct are of incorrect size, then there's no need to create
        // a list of names
        if (fields.size() != 2) {
            return false;
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        for (StructField s : fields) {
            fieldNames.add(s.getFieldName());
        }
        return (fieldNames.contains(OID)) && (fieldNames.contains("bsonType"));
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
                }
                return (String) obj;
            case TIMESTAMP:
                return new BSONTimestamp(((Long) (((Timestamp) obj).getTime() / 1000L)).intValue(), 1);
            case UNKNOWN:
            case VOID:
            default:
                return null;
        }
    }
}
