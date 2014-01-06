package com.mongodb.hadoop.pig;

import org.junit.Test;

import java.text.ParseException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoStorageOptionsTest {

    @Test
    public void testUpdate() {
        try {
            final String update1 = "update [string, stringTwo]";
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{update1});
            MongoStorageOptions.Index[] indexes = m.getIndexes();
            MongoStorageOptions.Update update = m.getUpdate();

            assertTrue("No insert condition provided, array should be empty", indexes.length == 0);
            assertNotNull("Update should not be null", update);
            assertFalse("Update multi option should be false, for 'update' string", update.multi);
            assertTrue("Query should contain field string but does not", update.keys[0].equals("string"));
            assertTrue("Query should contain field stringTwo but does not", update.keys[1].equals("stringTwo"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        }
    }

    @Test
    public void testMultiUpdate() {
        try {
            final String multiUpdate = "multi [string, stringTwo]";
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{multiUpdate});
            MongoStorageOptions.Index[] indexes = m.getIndexes();
            MongoStorageOptions.Update update = m.getUpdate();

            assertTrue("No insert condition provided, array should be empty", indexes.length == 0);
            assertNotNull("Update should not be null", update);
            assertTrue("Update multi option should be true, for 'multi' string", update.multi);
            assertTrue("Query should contain field string but does not", update.keys[0].equals("string"));
            assertTrue("Query should contain field stringTwo but does not", update.keys[1].equals("stringTwo"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        }
    }

    @Test
    public void testEnsureIndex() {
        try {
            final String insert = "{string : 1, stringTwo : -1},{}";
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insert});
            MongoStorageOptions.Index[] indexes = m.getIndexes();
            MongoStorageOptions.Update update = m.getUpdate();

            // Test proper result sizes returned
            assertTrue("Single insert provided, array should be length 1", indexes.length == 1);
            assertNull("Update not provided, should be null", update);
            MongoStorageOptions.Index index = indexes[0];

            // Test returned index is properly formed
            assertTrue("Index should contain field 'string' but does not", index.index.containsField("string"));
            assertTrue("Index should contain field 'stringTwo' but does not", index.index.containsField("stringTwo"));
            assertTrue("Index at 'string' should equal 1 but does not", (Integer) index.index.get("string") == 1);
            assertTrue("Index at 'string' should equal 1 but does not", (Integer) index.index.get("stringTwo") == -1);

            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertFalse("Default of unique should be false", (Boolean) index.options.get("unique"));
            assertFalse("Default of spare should be false", (Boolean) index.options.get("sparse"));
            assertFalse("Default of dropDups should be false", (Boolean) index.options.get("dropDups"));
            assertFalse("Default of background should be false", (Boolean) index.options.get("background"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        }
    }

    @Test
    public void testEnsureIndexUnique() {
        try {
            final String insertUnique = "{string : 1, stringTwo : 1},{unique : true}";
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insertUnique});
            MongoStorageOptions.Index index = m.getIndexes()[0];

            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertTrue("Unique should be true", (Boolean) index.options.get("unique"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        }
    }

    @Test
    public void testEnsureIndexSpare() {
        try {
            final String insertSparse = "{string : 1, stringTwo : 1},{sparse : true}";
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insertSparse});
            MongoStorageOptions.Index index = m.getIndexes()[0];

            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertTrue("spare should be true", (Boolean) index.options.get("sparse"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        }
    }

    @Test
    public void testEnsureIndexDropDups() {
        try {
            final String insertDropDups = "{string : 1, stringTwo : 1},{dropDups : true}";
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insertDropDups});
            MongoStorageOptions.Index index = m.getIndexes()[0];

            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertTrue("dropDups should be true", (Boolean) index.options.get("dropDups"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        }
    }

    @Test
    public void testEnsureIndexBackground() {
        try {
            final String insertBackground = "{string : 1, stringTwo : 1},{background : true}";
            MongoStorageOptions m = MongoStorageOptions.parseArguments(new String[]{insertBackground});
            MongoStorageOptions.Index index = m.getIndexes()[0];

            // Test that default options are correctly set
            assertNotNull("Options object not created properly", index.options);
            assertTrue("Background should be true", (Boolean) index.options.get("background"));
        } catch (ParseException e) {
            fail("Threw parse exception on valid string: " + e.getMessage());
        }
    }
}
