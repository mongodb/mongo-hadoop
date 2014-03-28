package com.mongodb.hadoop.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

@SuppressWarnings("rawtypes")
public class MongoLoaderTest {
    private static PigServer pigServerLocal = null;
    private static String dbName = "MongoLoaderTest-" + new Date().getTime();

    @AfterClass
    public static void tearDown() throws Exception {
        new MongoClient().dropDatabase(dbName);
    }
    
    private void insertData(String collectionName, BasicDBObject... objs) throws Exception {
        MongoClient mc = new MongoClient();
        DBCollection col = mc.getDB(dbName).getCollection(collectionName);
        col.insert(objs);
    }

    @Test
    public void testSimpleChararray_fullRun() throws Exception {
        BasicDBObject obj1 = new BasicDBObject("a", "value1");
        BasicDBObject obj2 = new BasicDBObject("a", "value2");
        insertData("testSimpleChararray", obj1, obj2);
        
        pigServerLocal = new PigServer(ExecType.LOCAL);
        
        //Test no schema
        pigServerLocal.registerQuery(String.format(
                "A = LOAD 'mongodb://localhost:27017/%s.%s' using com.mongodb.hadoop.pig.MongoLoader();", dbName, "testSimpleChararray"));

        Iterator<Tuple> iter = pigServerLocal.openIterator("A");
        assertTrue(iter.hasNext());
        Tuple result1 = iter.next();
        assertTrue(iter.hasNext());
        Tuple result2 = iter.next();
        assertFalse(iter.hasNext());
        
        Map map1 = (Map) result1.get(0);
        assertEquals(obj1.getString("a"), map1.get("a"));
        assertEquals(obj1.getObjectId("_id"), map1.get("_id"));

        Map map2 = (Map) result2.get(0);
        assertEquals(obj2.getString("a"), map2.get("a"));
        assertEquals(obj2.getObjectId("_id"), map2.get("_id"));
        
        
        //Test Schema and idAlias
        pigServerLocal.registerQuery(String.format(
                "B = LOAD 'mongodb://localhost:27017/%s.%s' using com.mongodb.hadoop.pig.MongoLoader('mongo_id:chararray, a:chararray', 'mongo_id');", dbName, "testSimpleChararray"));
        
        iter = pigServerLocal.openIterator("B");
        assertTrue(iter.hasNext());
        result1 = iter.next();
        assertTrue(iter.hasNext());
        result2 = iter.next();
        assertFalse(iter.hasNext());
        
        assertEquals(obj1.getObjectId("_id"), result1.get(0));
        assertEquals(obj1.getString("a"), result1.get(1));
        assertEquals(obj2.getObjectId("_id"), result2.get(0));
        assertEquals(obj2.getString("a"), result2.get(1));
        
        
        //Test loadaschararray
        pigServerLocal.registerQuery(String.format(
                "C = LOAD 'mongodb://localhost:27017/%s.%s' using com.mongodb.hadoop.pig.MongoLoader('s:chararray', '', '--loadaschararray');", dbName, "testSimpleChararray"));
        
        iter = pigServerLocal.openIterator("C");
        assertTrue(iter.hasNext());
        result1 = iter.next();
        assertTrue(iter.hasNext());
        result2 = iter.next();
        assertFalse(iter.hasNext());
        
        assertEquals( String.format("{ \"_id\" : { \"$oid\" : \"%s\"} , \"a\" : \"%s\"}", obj1.getObjectId("_id"), obj1.getString("a"))
                    , result1.get(0));
        assertEquals( String.format("{ \"_id\" : { \"$oid\" : \"%s\"} , \"a\" : \"%s\"}", obj2.getObjectId("_id"), obj2.getString("a"))
                    , result2.get(0));
    }
    
    @Test
    public void testEmbeddedObject_fullRun() throws Exception {
        // Input Data: {"f1":"v1", "f2":2, "f3": [1,2,3], "f4":{"i1":"inner1","i2":"inner2"}}
        BasicDBList innerBag = new BasicDBList();
        innerBag.addAll(Arrays.asList(1,2,3));
        BasicDBObject innerObj = new BasicDBObject()
                        .append("i1", "inner1")
                        .append("i2", 2);
        BasicDBObject obj = new BasicDBObject()
            .append("f1", "v1")
            .append("f2",  2)
            .append("f3", innerBag)
            .append("f4", innerObj)
        ;
        insertData("testEmbeddedObject", obj);
        
        pigServerLocal = new PigServer(ExecType.LOCAL);
        
        //Test no schema
        pigServerLocal.registerQuery(String.format(
                "A = LOAD 'mongodb://localhost:27017/%s.%s' using com.mongodb.hadoop.pig.MongoLoader();", dbName, "testEmbeddedObject"));

        Iterator<Tuple> iter = pigServerLocal.openIterator("A");
        assertTrue(iter.hasNext());
        Tuple result1 = iter.next();
        assertFalse(iter.hasNext());
        
        Map map1 = (Map) result1.get(0);
        assertEquals(obj.getString("_id"), map1.get("_id"));
        assertEquals(obj.getString("f1"), map1.get("f1"));
        assertEquals(obj.getInt("f2"), map1.get("f2"));
        
        DataBag bag = (DataBag) map1.get("f3");
        assertEquals(innerBag.size(), bag.size());
        assertEquals(1, bag.iterator().next().get(0));
        
        Map innerMap = (Map) map1.get("f4");
        assertEquals(innerObj.get("i1"), innerMap.get("i1"));
        assertEquals(innerObj.get("i2"), innerMap.get("i2"));
        
        
        //Test Schema and idAlias
        pigServerLocal.registerQuery(String.format(
                "B = LOAD 'mongodb://localhost:27017/%s.%s' using com.mongodb.hadoop.pig.MongoLoader('mongo_id:chararray, f1:chararray, f2:int, f3:bag{}, f4:(i1:chararray, i2:int)', 'mongo_id');", dbName, "testEmbeddedObject"));
        
        iter = pigServerLocal.openIterator("B");
        assertTrue(iter.hasNext());
        result1 = iter.next();
        assertFalse(iter.hasNext());
        
        assertEquals(obj.getObjectId("_id"), result1.get(0));
        assertEquals(obj.getString("f1"), result1.get(1));
        assertEquals(obj.get("f2"), result1.get(2));
        
        bag = (DataBag) result1.get(3);
        assertEquals(innerBag.size(), bag.size());
        assertEquals(1, bag.iterator().next().get(0));
        
        Tuple innerTuple = (Tuple) result1.get(4);
        assertEquals(innerObj.get("i1"), innerTuple.get(0));
        assertEquals(innerObj.get("i2"), innerTuple.get(1));
    }
    
    
}
