package com.mongodb.hadoop.pig;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.AfterClass;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoUpdateStorageTest {
    private static PigServer pigServerLocal = null;
    private static String dbName = "MongoUpdateStorageTest-" + new Date().getTime();

    @AfterClass
    public static void tearDown() throws Exception {
        new MongoClient().dropDatabase(dbName);
    }

    @Test
    public void testUpdate() throws Exception {
        BasicDBObject obj1 = new BasicDBObject()
                                   .append("f1", "a")
                                   .append("f2", "value1");
        BasicDBObject obj2 = new BasicDBObject()
                                   .append("f1", "b")
                                   .append("f2", "value2");
        insertData("testUpdate", obj1, obj2);

        String[] input = {
            "a\tnewValue1\t1",
            "b\tnewValue2\t2"
        };
        Util.createLocalInputFile("simple_input", input);
        
        pigServerLocal = new PigServer(ExecType.LOCAL);
        pigServerLocal.registerQuery("A = LOAD 'simple_input' as (f1:chararray, f2:chararray, f3:int);");
        pigServerLocal.registerQuery(String.format(
                "STORE A INTO 'mongodb://localhost:27017/%s.%s' USING com.mongodb.hadoop.pig.MongoUpdateStorage("
              + "  '{f1:\"\\\\$f1\"}',"
              + "  '{\\\\$set:{f2:\"\\\\$f2\", f3:\"\\\\$f3\"}}',"
              + "  'f1:chararray, f2:chararray, f3:int'"
              + ");", dbName, "update_simple"));
        pigServerLocal.setBatchOn();
        pigServerLocal.executeBatch();
        
        MongoClient mc = new MongoClient();
        DBCollection col = mc.getDB(dbName).getCollection("update_simple");
        
        DBCursor cursor = col.find();
        
        assertEquals(2, cursor.size());
        DBObject result1 = cursor.next();
        assertEquals("a", result1.get("f1"));
        assertEquals("newValue1", result1.get("f2"));
        assertEquals(1, result1.get("f3"));
        DBObject result2 = cursor.next();
        assertEquals("b", result2.get("f1"));
        assertEquals("newValue2", result2.get("f2"));
        assertEquals(2, result2.get("f3"));
    }
    
    private void insertData(String collectionName, BasicDBObject... objs) throws Exception {
        MongoClient mc = new MongoClient();
        DBCollection col = mc.getDB(dbName).getCollection(collectionName);
        col.insert(objs);
    }
}
