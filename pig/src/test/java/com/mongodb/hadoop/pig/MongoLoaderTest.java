package com.mongodb.hadoop.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.bson.BSONObject;
import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;


public class MongoLoaderTest {
    @Test
    public void testReadField_simpleChararray() throws IOException {
        String userSchema = "d:chararray";
        MongoLoader ml = new MongoLoader(userSchema);

        Object result = ml.readField("value", ml.fields[0]);
        assertEquals("value", result);
    }
    
    @Test
    public void testReadField_simpleFloat() throws IOException {
        String userSchema = "d:float";
        MongoLoader ml = new MongoLoader(userSchema);

        Object result = ml.readField(1.1F, ml.fields[0]);
        assertEquals(1.1F, result);
    }
    
    @Test
    public void testReadField_simpleFloatAsDouble() throws IOException {
        String userSchema = "d:float";
        MongoLoader ml = new MongoLoader(userSchema);

        Object result = ml.readField(1.1D, ml.fields[0]);
        assertEquals(1.1F, result);
    }
    
    @Test
    public void testReadField_simpleTuple() throws IOException {
        String userSchema = "t:tuple(t1:chararray, t2:chararray)";
        Object val = new BasicDBObject()
            .append("t1", "t1_value")
            .append("t2", "t2_value");
        MongoLoader ml = new MongoLoader(userSchema);

        Object result = ml.readField(val, ml.fields[0]);
        
        Tuple t = (Tuple) result;
        assertEquals(2, t.size());
        assertEquals("t1_value", t.get(0));
        assertEquals("t2_value", t.get(1));
    }
    
    @Test
    public void testReadField_simpleTupleMissingField() throws IOException {
        String userSchema = "t:tuple(t1:chararray, t2:chararray, t3:chararray)";
        Object val = new BasicDBObject()
            .append("t1", "t1_value")
            .append("t2", "t2_value");
        MongoLoader ml = new MongoLoader(userSchema);

        Object result = ml.readField(val, ml.fields[0]);
        
        Tuple t = (Tuple) result;
        assertEquals(3, t.size());
        assertEquals("t1_value", t.get(0));
        assertEquals("t2_value", t.get(1));
        assertNull(t.get(2));
    }
    
    @Test
    public void testReadField_simpleTupleIncorrectFieldType() throws IOException {
        String userSchema = "t:tuple(t1:chararray, t2:float)";
        Object val = new BasicDBObject()
            .append("t1", "t1_value")
            .append("t2", "t2_value");
        MongoLoader ml = new MongoLoader(userSchema);

        Object result = ml.readField(val, ml.fields[0]);
        
        Tuple t = (Tuple) result;
        assertEquals(2, t.size());
        assertEquals("t1_value", t.get(0));
        assertNull(t.get(1));
    }
    
    @Test
    public void testReadField_listAsTuple() throws IOException {
        String userSchema = "b:{t:(f1:int, f2:int)}";
        BasicDBList bag = new BasicDBList();
        BasicDBList t1 = new BasicDBList();
        t1.add(1);
        t1.add(2);
        BasicDBList t2 = new BasicDBList();
        t2.add(3);
        t2.add(4);
        bag.add(t1);
        bag.add(t2);
        MongoLoader ml = new MongoLoader(userSchema);
        
        Object result = ml.readField(bag, ml.fields[0]);
        
        DataBag b = (DataBag) result;
        Iterator<Tuple> bit = b.iterator();
        
        Tuple firstInnerT = bit.next();
        assertEquals(2, firstInnerT.size());
        assertEquals(1, firstInnerT.get(0));
        assertEquals(2, firstInnerT.get(1));

        Tuple secondInnerT = bit.next();
        assertEquals(2, secondInnerT.size());
        assertEquals(3, secondInnerT.get(0));
        assertEquals(4, secondInnerT.get(1));
    }
    
    @Test
    public void testReadField_listOfUnnamedSimpleObjects() throws IOException {
        String userSchema = "b:{t:()}";
        BasicDBList bag = new BasicDBList();
        bag.add("val1");
        bag.add("val2");
        MongoLoader ml = new MongoLoader(userSchema);

        Object result = ml.readField(bag, ml.fields[0]);
        
        DataBag b = (DataBag) result;
        Iterator<Tuple> bit = b.iterator();
        
        Tuple firstInnerT = bit.next();
        assertEquals(1, firstInnerT.size());
        assertEquals("val1", firstInnerT.get(0));

        Tuple secondInnerT = bit.next();
        assertEquals(1, secondInnerT.size());
        assertEquals("val2", secondInnerT.get(0));

        assertFalse(bit.hasNext());
    }
    
    @Test
    public void testReadField_simpleBag() throws IOException {
        String userSchema = "b:{(t1:chararray, t2:chararray)}";
        BasicDBList bag = new BasicDBList();
        bag.add(new BasicDBObject()
                        .append("t1", "t11_value")
                        .append("t2", "t12_value"));
        bag.add(new BasicDBObject()
                        .append("t1", "t21_value")
                        .append("t2", "t22_value"));
        MongoLoader ml = new MongoLoader(userSchema);

        Object result = ml.readField(bag, ml.fields[0]);
        
        DataBag b = (DataBag) result;
        Iterator<Tuple> bit = b.iterator();
        
        Tuple firstInnerT = bit.next();
        assertEquals(2, firstInnerT.size());
        assertEquals("t11_value", firstInnerT.get(0));
        assertEquals("t12_value", firstInnerT.get(1));
        
        Tuple secondInnerT = bit.next();
        assertEquals(2, secondInnerT.size());
        assertEquals("t21_value", secondInnerT.get(0));
        assertEquals("t22_value", secondInnerT.get(1));
        
        assertFalse(bit.hasNext());
    }
    
    @Test
    public void testReadField_bagThatIsNotABag() throws IOException {
        String userSchema = "b:{(t1:chararray, t2:chararray)}";
        BasicDBObject notABag = new BasicDBObject();
        notABag.append("f1", new BasicDBObject()
                        .append("t1", "t11_value")
                        .append("t2", "t12_value"));
        notABag.append("f2", new BasicDBObject()
                        .append("t1", "t21_value")
                        .append("t2", "t22_value"));
        MongoLoader ml = new MongoLoader(userSchema);

        Object result = ml.readField(notABag, ml.fields[0]);
        assertNull(result);
    }
    
    @Test
    public void testReadField_deepness() throws IOException {
        String userSchema = "b:{(t1:chararray, b:{(i1:int, i2:int)})}";
        
        BasicDBList innerBag = new BasicDBList();
        innerBag.add(new BasicDBObject()
                        .append("i1", 1)
                        .append("i2", 2));
        innerBag.add(new BasicDBObject()
                        .append("i1", 3)
                        .append("i2", 4));

        BasicDBList bag = new BasicDBList();
        bag.add(new BasicDBObject()
                    .append("t1", "t1_value")
                    .append("b", innerBag));

        MongoLoader ml = new MongoLoader(userSchema);

        DataBag result = (DataBag) ml.readField(bag, ml.fields[0]);
        assertEquals(1, result.size());
        
        Iterator<Tuple> bit = result.iterator();
        Tuple t = bit.next();
        
        assertEquals(2, t.size());
        
        DataBag innerBagResult = (DataBag) t.get(1);
        assertEquals(2, innerBagResult.size());
        
        Iterator<Tuple> innerBit = innerBagResult.iterator();
        Tuple innerT = innerBit.next();
        
        assertEquals(2, innerT.get(1));
    }
    
    @Test
    public void testReadField_simpleMapNoSchema() throws Exception {
        String userSchema = "m:[]";
        BasicDBObject obj = new BasicDBObject()
            .append("k1", 1)
            .append("k2", 2)
            .append("k3", 3.0)
            .append("k4", 4L)
            .append("k5", 5.0f);
        
        MongoLoader ml = new MongoLoader(userSchema);
        Map m = (Map) ml.readField(obj, ml.fields[0]);

        assertEquals(5, m.size());
        assertEquals(1, m.get("k1"));
        assertEquals(2, m.get("k2"));
        assertEquals(3.0, m.get("k3"));
        assertEquals(4L, m.get("k4"));
        assertEquals(5.0f, m.get("k5"));
    }
    
    @Test
    public void testReadField_simpleMap() throws Exception {
        String userSchema = "m:[int]";
        BasicDBObject obj = new BasicDBObject()
            .append("k1", 1)
            .append("k2", 2);
        
        MongoLoader ml = new MongoLoader(userSchema);
        Map m = (Map) ml.readField(obj, ml.fields[0]);

        assertEquals(2, m.size());
        assertEquals(1, m.get("k1"));
        assertEquals(2, m.get("k2"));
    }
    
    @Test
    public void testReadField_mapWithTuple() throws Exception {
        String userSchema = "m:[(t1:chararray, t2:int)]";
        BasicDBObject v1 = new BasicDBObject()
            .append("t1", "t11 value")
            .append("t2", 12);
        BasicDBObject v2 = new BasicDBObject()
            .append("t1", "t21 value")
            .append("t2", 22);
        BasicDBObject obj = new BasicDBObject()
            .append("v1", v1)
            .append("v2", v2);
        
        MongoLoader ml = new MongoLoader(userSchema);
        Map m = (Map) ml.readField(obj, ml.fields[0]);

        assertEquals(2, m.size());
        
        Tuple t1 = (Tuple) m.get("v1");
        assertEquals("t11 value", t1.get(0));
        assertEquals(12, t1.get(1));
        
        Tuple t2 = (Tuple) m.get("v2");
        assertEquals("t21 value", t2.get(0));
    }
    
    @Test
    public void testReadField_mapWithMap_noSchema() throws Exception {
        BasicDBObject v1 = new BasicDBObject()
            .append("t1", "t11 value")
            .append("t2", 12);
        BasicDBObject v2 = new BasicDBObject()
            .append("t1", "t21 value")
            .append("t2", 22);
        BasicDBObject obj = new BasicDBObject()
            .append("v1", v1)
            .append("v2", v2);
        
        MongoLoader ml = new MongoLoader();
        Map m = (Map) ml.readField(obj, ml.schema.getFields()[0]);

        assertEquals(2, m.size());
        
        Map m1 = (Map) m.get("v1");
        assertEquals("t11 value", m1.get("t1"));
        assertEquals(12, m1.get("t2"));
        
        Map m2 = (Map) m.get("v2");
        assertEquals("t21 value", m2.get("t1"));
    }
    
    @Test
    public void testReadField_mapWithList_noSchema() throws Exception {
        BasicDBObject v1 = new BasicDBObject()
            .append("t1", "t1 value")
            .append("t2", 12);
        BasicDBObject v2 = new BasicDBObject()
            .append("t1", "t1 value")
            .append("t2", 22);
        BasicDBList vl = new BasicDBList();
        vl.add(v1);
        vl.add(v2);
        
        BasicDBObject obj = new BasicDBObject()
            .append("some_list", vl);

        MongoLoader ml = new MongoLoader();
        Map m = (Map) ml.readField(obj, ml.schema.getFields()[0]);

        assertEquals(1, m.size());
        
        DataBag bag = (DataBag) m.get("some_list");
        assertEquals(2, bag.size());
        
        Iterator<Tuple> bit = bag.iterator();
        Tuple t = bit.next();
        
        assertEquals(1, t.size());
        
        Map innerMap = (Map) t.get(0);
        assertEquals("t1 value", innerMap.get("t1"));
    }
    
    @Test
    public void testReadField_mapWithSimpleList_noSchema() throws Exception {
        BasicDBList vl = new BasicDBList();
        vl.add("v1");
        vl.add("v2");
        
        BasicDBObject obj = new BasicDBObject()
            .append("some_list", vl);

        MongoLoader ml = new MongoLoader();
        Map m = (Map) ml.readField(obj, ml.schema.getFields()[0]);

        assertEquals(1, m.size());
        
        DataBag bag = (DataBag) m.get("some_list");
        assertEquals(2, bag.size());
        
        Iterator<Tuple> bit = bag.iterator();
        Tuple t = bit.next();
        
        assertEquals(1, t.size());
        assertEquals("v1", t.get(0));
        
        t = bit.next();
        assertEquals(1, t.size());
        assertEquals("v2", t.get(0));
    }
    
    @Test
    public void testReadField_noUserSchema() throws Exception {
        BSONObject val = new BasicDBObject()
            .append("t1", "t11 value")
            .append("t2", 12);
        
        MongoLoader ml = new MongoLoader();
        Map result = (Map) ml.readField(val, ml.schema.getFields()[0]);
        assertEquals(2, result.size());
        assertEquals("t11 value", result.get("t1"));
        assertEquals(12, result.get("t2"));
    }
}
