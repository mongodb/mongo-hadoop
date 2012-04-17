package com.mongodb.hadoop.pig;

import static org.junit.Assert.*;

import java.text.ParseException;

import org.junit.Test;

import com.mongodb.hadoop.pig.MongoStorageOptions;

public class MongoStorageOptionsTest {
    String update = "update [string, stringtwo]";
    String multiupdate = "multi [string, stringtwo]";
    String insert = "{string : 1, stringtwo : -1},{}";
    String insertUnique = "{string : 1, stringtwo : 1},{unique : true}";
    String insertSparse = "{string : 1, stringtwo : 1},{sparse : true}";
    String insertDropDups = "{string : 1, stringtwo : 1},{dropDups : true}";
    String insertBackground = "{string : 1, stringtwo : 1},{background : true}";
    
    @Test
    public void TestUpdate() {
        try {
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{update});
            MongoStorageOptions.Index[] indexs = m.getIndexes();
            MongoStorageOptions.Update update = m.getUpdate();
            
            assertTrue("No insert condition provided, array should be empty", indexs.length == 0);
            assertNotNull("Update should not be null", update);
            assertFalse("Update multi option should be false, for 'update' string", update.multi);
            for (String key : update.keys) {
                System.out.println(key);
            }
            assertTrue("Query should contain field string but does not", update.keys[0].equals("string"));
            assertTrue("Query should contain field stringtwo but does not", update.keys[1].equals("stringtwo"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        }
    }
    
    @Test
    public void TestMultiUpdate() {
        try {
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{multiupdate});
            MongoStorageOptions.Index[] indexs = m.getIndexes();
            MongoStorageOptions.Update update = m.getUpdate();
            
            assertTrue("No insert condition provided, array should be empty", indexs.length == 0);
            assertNotNull("Update should not be null", update);
            assertTrue("Update multi option should be true, for 'multi' string", update.multi);
            assertTrue("Query should contain field string but does not", update.keys[0].equals("string"));
            assertTrue("Query should contain field stringtwo but does not", update.keys[1].equals("stringtwo"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        };
    }
    
    @Test
    public void TestEnsureIndex() {
        try {
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insert});
            MongoStorageOptions.Index[] indexs = m.getIndexes();
            MongoStorageOptions.Update update = m.getUpdate();
            
            // Test proper result sizes returned
            assertTrue("Single insert provided, array should be length 1", indexs.length == 1);
            assertNull("Update not provided, should be null", update);
            MongoStorageOptions.Index index = indexs[0];
            
            // Test returned index is properly formed
            assertTrue("Index should contain field 'string' but does not", index.index.containsField("string"));
            assertTrue("Index should contain field 'stringtwo' but does not", index.index.containsField("stringtwo"));
            assertTrue("Index at 'string' should equal 1 but does not", (Integer)index.index.get("string") == 1);
            assertTrue("Index at 'string' should equal 1 but does not", (Integer)index.index.get("stringtwo") == -1);
            
            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertFalse("Default of unique should be false", (Boolean)index.options.get("unique"));
            assertFalse("Default of spare should be false", (Boolean)index.options.get("sparse"));
            assertFalse("Default of dropDups should be false", (Boolean)index.options.get("dropDups"));
            assertFalse("Default of background should be false", (Boolean)index.options.get("background"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        };
    }
    
    @Test
    public void TestEnsureIndexUnique() {
        try {
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insertUnique});
            MongoStorageOptions.Index index = m.getIndexes()[0];
            
            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertTrue("Unique should be true", (Boolean)index.options.get("unique"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        };
    }
    
    @Test
    public void TestEnsureIndexSpare() {
        try {
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insertSparse});
            MongoStorageOptions.Index index = m.getIndexes()[0];
            
            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertTrue("spare should be true", (Boolean)index.options.get("sparse"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        };
    }
    
    @Test
    public void TestEnsureIndexDropDups() {
        try {
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insertDropDups});
            MongoStorageOptions.Index index = m.getIndexes()[0];
            
            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertTrue("dropDups should be true", (Boolean)index.options.get("dropDups"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        };
    }
    
    @Test
    public void TestEnsureIndexBackground() {
        try {
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insertBackground});
            MongoStorageOptions.Index index = m.getIndexes()[0];
            
            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertTrue("Background should be true", (Boolean)index.options.get("background"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        };
    }
}
