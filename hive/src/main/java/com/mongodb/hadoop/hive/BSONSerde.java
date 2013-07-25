package com.mongodb.hadoop.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.io.BSONWritable;

public class BSONSerde extends AbstractSerDe {

    
    public boolean DEBUG = true;
    private static final Log LOG = LogFactory.getLog(BSONSerde.class.getName());

    private StructTypeInfo docTypeInfo;
    private ObjectInspector docOI;
    private List<String> columnNames;
    private List<TypeInfo> columnTypes;

    private List<Object> row = new ArrayList<Object>();

    /**
     * Finds out the information of the table, including the column names and types. 
     */
    @Override
    public void initialize(Configuration conf, Properties tblProps)
            throws SerDeException {

        // Gets the column names
        String colNamesStr = tblProps.getProperty(serdeConstants.LIST_COLUMNS);
        columnNames = Arrays.asList(colNamesStr.split(","));

        // Get the column types
        String colTypesStr = tblProps.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);

        assert( columnNames.size() == columnTypes.size()) :
            "Column Names and Types don't match in size";

        // Get the structure and object inspector
        docTypeInfo = 
                (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        docOI = 
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(docTypeInfo);
        
        if (DEBUG) {
            for (int i = 0 ; i < columnTypes.size() ; i++) {
                /*System.out.println("initilize--" + columnNames.get(i) + ":" 
            + columnTypes.get(i).getTypeName());*/
            }
        }
    }

    /**
     * Given a Writable object of BSON, turn it into a table
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
                    " requires a BSONWritable object, not" + writ.getClass());
        }
        
        if (DEBUG) {
            System.out.println(doc.toString());
        }

        // Only lower case names
        BSONObject lower = new BasicBSONObject();
        for (Entry<String, Object> entry : ((BasicBSONObject) doc).entrySet()) {
            if (lower.containsField(entry.getKey().toLowerCase())) {
                LOG.error("Fields should only be lower cased and not duplicated: " 
                        + entry.getKey());
            } else {
                lower.put(entry.getKey().toLowerCase(), entry.getValue());
            }
        }

        Object value = null;
        for (String fieldName : docTypeInfo.getAllStructFieldNames()) {
            try {
                TypeInfo fieldTypeInfo = docTypeInfo.getStructFieldTypeInfo(fieldName);
                value = parseField(lower.get(fieldName), fieldTypeInfo);
                if (DEBUG) {
                    System.out.println("deserialize--" + fieldName + ":" + value.toString());
                }
            } catch (Exception e) {
                value = null;
            }
            row.add(value);
        }


        if (DEBUG) {
            System.out.println("1 writable done");
        }
        return row;
    }

    /**
     * For a given Object value and its supposed TypeInfo
     * determine and return its Java object representation
     */
    private Object parseField(Object value, TypeInfo valueTypeInfo) {

        if (value == null) {
            return null;
        }
        
        if (DEBUG) {
            System.out.println("Field-- " +value.toString() + ":" + valueTypeInfo.getCategory().toString());
        }
        switch (valueTypeInfo.getCategory()) {
            case PRIMITIVE:
                return parsePrimitive(value, valueTypeInfo);
            case LIST:
                return parseList(value, (ListTypeInfo) valueTypeInfo);
            case MAP:
                return parseMap(value, (MapTypeInfo) valueTypeInfo);
            case STRUCT:
                // Mongo has no struct
                break;
            case UNION:
                // Mongo also has no union
                return null;
            default:
                return parseMongoType(value);
                // Must be an unknown (a Mongo specific type)
                
        }
        return value;
    }
    
    private Object parsePrimitive(Object value, TypeInfo valueTypeInfo) {

        String typeName = valueTypeInfo.getTypeName();        
        if (DEBUG) {
            System.out.println("PRIMITIVE-- " + value.getClass().toString() + ":" + typeName);
        }
        if (typeName.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {
            return (Double) value;
        } else if (typeName.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
            return (Long) value;
        } else if (typeName.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)) {
            if (value instanceof Double) {
                return ((Double) value).intValue();
            } else if (value instanceof Float) {
                return ((Float) value).intValue();
            }
            return (Integer) value;
        } else if (typeName.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)) {
            return (Byte) value;
        } else if (typeName.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
            return (Float) value;
        } else if (typeName.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
            return (Boolean) value;
        } else if (typeName.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)) {
            return (String) value;
        } else if (typeName.equalsIgnoreCase(serdeConstants.DATE_TYPE_NAME)
                || typeName.equalsIgnoreCase(serdeConstants.DATETIME_TYPE_NAME)
                || typeName.equalsIgnoreCase(serdeConstants.TIMESTAMP_TYPE_NAME)) {
            return (Date) value;
        } else {
            LOG.equals("not a known primitive type");
        }
        return value;
    }
    
    private Object parseList(Object value, ListTypeInfo valueTypeInfo) {
        BasicBSONList list = (BasicBSONList) value;
        TypeInfo listTypeInfo = valueTypeInfo.getListElementTypeInfo();
        if (DEBUG) {
            System.out.println("in list: " + listTypeInfo.getTypeName());
        }
        
        for (int i = 0 ; i < list.size() ; i++) {
            list.set(i, parseField(list.get(i), listTypeInfo));
        }
        return list.toArray();
    }
    
    private Object parseMap(Object value, MapTypeInfo valueTypeInfo) {
        BasicBSONObject b = (BasicBSONObject) value;
        TypeInfo mapTypeInfo = valueTypeInfo.getMapValueTypeInfo();
        if (DEBUG) {
            System.out.println("in map: " + mapTypeInfo.getTypeName());
        }
        
        for (Entry<String, Object> entry : b.entrySet()) {
            b.put(entry.getKey(), parseField(entry.getValue(), mapTypeInfo));
        }
        
        return b.toMap();
    }
    
    /**
     * 
     * For Mongo Specific types, return the most appropriate java types
     */
    private Object parseMongoType(Object value) {
        if (value instanceof ObjectId) {
            return value.toString();
        }
        return null;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return docOI;
    }

    @Override
    public SerDeStats getSerDeStats() {
        //TODO:: this needs to be determined what it is.
        return null;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return BSONWritable.class;
    }

    @Override
    public Writable serialize(Object arg0, ObjectInspector arg1)
            throws SerDeException {
        // TODO Auto-generated method stub
        return null;
    }

}
