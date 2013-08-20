package com.mongodb.hadoop.hive;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.junit.Test;

import com.mongodb.hadoop.io.BSONWritable;

public class BSONSerDeTest {    

    /**
     * Given the column names and types, set the table properties and create a serde
     * then deserialize the value according to the first field
     * 
     */
    private Object helpDeserialize(BSONSerDe serde, String columnNames, String columnTypes, 
            Object value, boolean isStruct) throws SerDeException {
        Properties tblProperties = new Properties();
        tblProperties.setProperty(serdeConstants.LIST_COLUMNS, columnNames);
        tblProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypes);
        
        serde.initialize(new Configuration(), tblProperties);
        
        return serde.deserializeField(value, serde.columnTypes.get(0), 
                isStruct ? columnNames : "");
    }
    
    private Object helpDeserialize(BSONSerDe serde, String columnNames, String columnTypes, 
            Object value) throws SerDeException {
        return helpDeserialize(serde, columnNames, columnTypes, value, false);
    }
    

    /**
     * Given the column names and the object inspector, returns the 
     * struct object inspector, Notice how the fieldNames and the fieldInspectors
     * are both Lists
     */
    private StructObjectInspector createObjectInspector(String columnNames, ObjectInspector oi) {
        ArrayList<String> fieldNames = new ArrayList<String>();
        fieldNames.add(columnNames);
        ArrayList<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        fieldInspectors.add(oi);
        
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
    }

    
    /**
     * 
      * Given the column names and the object inspector, the serialized
      * object result. Notice how the fieldNames and the fieldInspectors
     * are both Lists. 
     */
    private Object helpSerialize(String columnNames, ObjectInspector inner, 
            BasicBSONObject bObject, Object value, BSONSerDe serde) 
            throws SerDeException {
        
        StructObjectInspector oi = createObjectInspector(columnNames, inner);
        bObject.put(columnNames, value);
        // Structs in Hive are actually arrays/lists of objects
        ArrayList<Object> obj = new ArrayList<Object>();
        obj.add(value);
        return serde.serialize(obj, oi);
    }
    
    
    @Test
    public void testString() throws SerDeException {        
        
        String columnNames = "s";
        String columnTypes = "string";
        String value = "value";
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, value);
        assertThat(value, equalTo(result));
        
        ObjectInspector innerInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(String.class);
        BasicBSONObject bObject = new BasicBSONObject();
        Object serialized = helpSerialize(columnNames, innerInspector, bObject, value, serde);
        assertThat(new BSONWritable(bObject), equalTo(serialized));
    }
    
    
    @Test
    public void testDouble() throws SerDeException {
        
        String columnNames = "doub";
        String columnTypes = "double";
        Double value = 1.1D;
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, value);
        assertThat(value, equalTo(result));
        
        ObjectInspector innerInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(Double.class);
        BasicBSONObject bObject = new BasicBSONObject();
        Object serialized = helpSerialize(columnNames, innerInspector, bObject, value, serde);
        assertThat(new BSONWritable(bObject), equalTo(serialized));
    }
    
    
    @Test
    public void testInt() throws SerDeException {
        
        String columnNames = "i";
        String columnTypes = "int";
        Integer value = 1234;
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, value);
        assertThat(value, equalTo(result));
        
        ObjectInspector innerInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(Integer.class);
        BasicBSONObject bObject = new BasicBSONObject();
        Object serialized = helpSerialize(columnNames, innerInspector, bObject, value, serde);
        assertThat(new BSONWritable(bObject), equalTo(serialized));
    }
    
    
    @Test
    public void testBinary() throws SerDeException {
        
        String columnNames = "b";
        String columnTypes = "binary";
        byte[] value = new byte[2];
        value[0] = 'A';
        value[1] = '1';
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, value);
        assertThat(value, equalTo(result));
        
        ObjectInspector innerInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(byte[].class);
        BasicBSONObject bObject = new BasicBSONObject();
        Object serialized = helpSerialize(columnNames, innerInspector, bObject, value, serde);
        assertThat(new BSONWritable(bObject), equalTo(serialized));
    }
    
    
    @Test
    public void testBoolean() throws SerDeException {
        
        String columnNames = "bool";
        String columnTypes = "boolean";        
        Boolean value = false;
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, value);
        assertThat(value, equalTo(result));
        
        ObjectInspector innerInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(Boolean.class);
        BasicBSONObject bObject = new BasicBSONObject();
        Object serialized = helpSerialize(columnNames, innerInspector, bObject, value, serde);
        assertThat(new BSONWritable(bObject), equalTo(serialized));
    }
    
    
    @Test
    public void testDates() throws SerDeException {
        
        String columnNames = "d";
        String columnTypes = "timestamp";   
        Date d = new Date();
        Timestamp value = new Timestamp(d.getTime());
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, value);
        assertThat(value, equalTo(result));
        
        result = serde.deserializeField(d, serde.columnTypes.get(0), "");
        assertThat(value, equalTo(result));
        
        BSONTimestamp bts = new BSONTimestamp(((Long) (d.getTime() / 1000L)).intValue(), 1);
        result = serde.deserializeField(bts, serde.columnTypes.get(0), "");
        // BSONTimestamp only takes an int, so the long returned in the Timestamp won't be the same
        assertThat((long) bts.getTime(), equalTo(((Timestamp) result).getTime() / 1000L));
        
        // Utilizes a timestampWritable because there's no native timestamp type in java for
        // object inspector class to relate to
        ObjectInspector innerInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(TimestampWritable.class);
        BasicBSONObject bObject = new BasicBSONObject();
        Object serialized = helpSerialize(columnNames, innerInspector, bObject, value, serde);
        
        // The time going in to serialize is Timestamp but it comes out as BSONTimestamp
        BasicBSONObject bsonWithTimestamp = new BasicBSONObject();
        bsonWithTimestamp.put(columnNames, bts);
        assertThat(new BSONWritable(bsonWithTimestamp), equalTo(serialized));
    }
    
    
    @Test
    public void testObjectID() throws SerDeException {
        
        String columnNames = "o";
        String columnTypes = "struct<oid:string,bsontype:int>";        
        ObjectId value = new ObjectId();
        ArrayList<Object> returned = new ArrayList<Object>(2);
        returned.add(value.toString());
        returned.add(8);
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, value);
        assertThat(returned, equalTo(result));
        

        // Since objectid is currently taken to be a string
        ObjectInspector innerInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(String.class);
        BasicBSONObject bObject = new BasicBSONObject();
        Object serialized = helpSerialize(columnNames, innerInspector, bObject, value.toString(), serde);
        assertThat(new BSONWritable(bObject), equalTo(serialized));
    }
    
    
    @Test 
    public void testList() throws SerDeException {
        String columnNames = "a";
        String columnTypes = "array<string>";
        
        String inner = "inside";
        ArrayList<String> value = new ArrayList<String>();
        value.add(inner);
        BasicBSONList b = new BasicBSONList();
        b.add(inner);
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, b);
        assertThat(value.toArray(), equalTo(result));
        
        // Since objectid is currently taken to be a string
        ObjectInspector innerInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(String.class);
        ListObjectInspector listInspector = 
                ObjectInspectorFactory.getStandardListObjectInspector(innerInspector);
        BasicBSONObject bObject = new BasicBSONObject();
        Object serialized = helpSerialize(columnNames, listInspector, bObject, value, serde);
        assertThat(new BSONWritable(bObject), equalTo(serialized));
    }
    
    
    @Test
    public void testMap() throws SerDeException {
        String columnNames = "m";
        String columnTypes = "map<string,int>";

        BasicBSONObject value = new BasicBSONObject();
        String oneKey = "one";
        int oneValue = 10;
        value.put(oneKey, oneValue);
        String twoKey = "two";
        int twoValue = 20;
        value.put(twoKey, twoValue);
        
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, value);
        assertThat(value.toMap(), equalTo(result));
        
        // Since objectid is currently taken to be a string
        ObjectInspector keyInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(String.class);
        ObjectInspector valueInspector = 
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(Integer.class);
        
        MapObjectInspector mapInspector = 
                ObjectInspectorFactory.getStandardMapObjectInspector(keyInspector, valueInspector);
        BasicBSONObject bObject = new BasicBSONObject();
        Object serialized = helpSerialize(columnNames, mapInspector, bObject, value, serde);
        assertThat(new BSONWritable(bObject), equalTo(serialized));
    }
    
    
    @Test
    public void testStruct() throws SerDeException {
        String columnNames = "m";
        String columnTypes = "struct<one:int,two:string>";
        
        BasicBSONObject value = new BasicBSONObject();
        int oneValue = 10;
        String twoValue = "key";
        value.put("one", oneValue);
        value.put("two", twoValue);
        
        // Structs come back as arrays
        ArrayList<Object> returned = new ArrayList<Object>();
        returned.add(oneValue);
        returned.add(twoValue);
        BSONSerDe serde = new BSONSerDe();
        Object result = helpDeserialize(serde, columnNames, columnTypes, value, true);
        assertThat(returned, equalTo(result));
        
        
        // A struct must have an array or list of inner inspector types
        ArrayList<ObjectInspector> innerInspectorList = new ArrayList<ObjectInspector>();
        innerInspectorList.add(PrimitiveObjectInspectorFactory.
                getPrimitiveObjectInspectorFromClass(Integer.class));
        innerInspectorList.add(PrimitiveObjectInspectorFactory.
                getPrimitiveObjectInspectorFromClass(String.class));
        
        // As well as a fields list
        ArrayList<String> innerFieldsList = new ArrayList<String>();
        innerFieldsList.add("one");
        innerFieldsList.add("two");
        // Then you get that inner struct's inspector
        StructObjectInspector structInspector = ObjectInspectorFactory.
                getStandardStructObjectInspector(innerFieldsList, innerInspectorList);
        // Which is used to get the overall struct inspector
        StructObjectInspector oi = createObjectInspector(columnNames, structInspector);
        
        // This should be how it turns out
        BasicBSONObject bObject = new BasicBSONObject();
        bObject.put(columnNames, value);
        
        // But structs are stored as array/list inside hive, so this is passed in
        ArrayList<Object> obj = new ArrayList<Object>();
        obj.add(returned);
        
        Object serialized = serde.serialize(obj, oi);
        assertThat(new BSONWritable(bObject), equalTo(serialized));
    }
}
