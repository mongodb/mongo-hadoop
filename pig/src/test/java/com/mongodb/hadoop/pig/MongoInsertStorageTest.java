package com.mongodb.hadoop.pig;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.test.Util;
import org.junit.AfterClass;
import org.junit.Test;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoInsertStorageTest {
    private static PigServer pigServerLocal = null;
    private static String dbName = "MongoInsertStorageTest-" + new Date().getTime();
    
    @AfterClass
    public static void tearDown() throws Exception {
        new MongoClient().dropDatabase(dbName);
    }
    
    @Test
    public void testInsert() throws Exception {
        String[] input = {
            "f11\t1",
            "f12\t2"
        };
        Util.createLocalInputFile("simple_input", input);
        
        pigServerLocal = new PigServer(ExecType.LOCAL);
        pigServerLocal.registerQuery("A = LOAD 'simple_input' as (f1:chararray, f2:int);");
        pigServerLocal.registerQuery(String.format(
                "STORE A INTO 'mongodb://localhost:27017/%s.%s' USING com.mongodb.hadoop.pig.MongoInsertStorage();", dbName, "insert_simple"));
        pigServerLocal.setBatchOn();
        pigServerLocal.executeBatch();
        
        MongoClient mc = new MongoClient();
        DBCollection col = mc.getDB(dbName).getCollection("insert_simple");
        
        DBCursor cursor = col.find();
        
        assertEquals(2, cursor.size());
        DBObject result1 = cursor.next();
        assertEquals("f11", result1.get("f1"));
        assertEquals(1, result1.get("f2"));
        DBObject result2 = cursor.next();
        assertEquals("f12", result2.get("f1"));
        assertEquals(2, result2.get("f2"));
    }
    

}
