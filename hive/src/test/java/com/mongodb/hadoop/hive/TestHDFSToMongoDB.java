package com.mongodb.hadoop.hive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.MongoClientURIBuilder;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestHDFSToMongoDB extends HiveTest {

    @Before
    public void setUp() {
        loadDataIntoHDFSHiveTable();
        loadDataIntoMongoDBHiveTable(false);
    }

    @After
    public void tearDown() {
        dropTable(MONGO_TEST_COLLECTION);
        dropTable(HIVE_TEST_TABLE);
    }


    @Test
    public void testSameDataHDFSAndMongoHiveTables() {
        Results hiveData = getAllDataFromTable(HIVE_TEST_TABLE);
        Results mongoData = getAllDataFromTable(MONGO_TEST_COLLECTION);
        assertNotEquals(hiveData.size(), 0);
        assertNotEquals(mongoData.size(), 0);

        assertEquals(hiveData, mongoData);
    }

    @Test
    public void testDeleteReflectData() {
        Results results = getAllDataFromTable(MONGO_TEST_TABLE);

        int size = results.size();
        assertTrue(size > 0);

        List<String> t = results.get(new Random().nextInt(size));
        DBObject toDelete = new BasicDBObject();
        int i = 0;
        for (FieldSchema schema : results.getFields()) {
            // add more types as necessary
            if (schema.getType().equals("int")) {
                toDelete.put(schema.getName(), Integer.valueOf(t.get(i)));
            } else if (schema.getType().equals("string")) {
                toDelete.put(schema.getName(), t.get(i));
            } else {
                toDelete.put(schema.getName(), t.get(i));
            }
            i++;
        }

        deleteFromCollection(toDelete);

        // get data from table now that the first row has been removed
        Results newResults = getAllDataFromTable(MONGO_TEST_TABLE);

        // now make sure that 'toDelete' doesn't exist anymore
        for (List<String> newResult : newResults) {
            assertNotEquals(newResult, t);
        }
    }

    private void deleteFromCollection(final DBObject toDelete) {
        getCollection().remove(toDelete);
    }

    @Test
    public void testDropReflectData() {
        assertTrue(getAllDataFromTable(MONGO_TEST_TABLE).size() > 0);
        getCollection().drop();
        assertEquals(0, getAllDataFromTable(MONGO_TEST_TABLE).size());
    }

    @Test
    public void testJOINHDFSMongoDB() {
        Results mongoTblData = getAllDataFromTable(MONGO_TEST_TABLE);
        Results hiveTblData = getAllDataFromTable(HIVE_TEST_TABLE);
        assertNotEquals(hiveTblData.size(), 0);
        assertNotEquals(mongoTblData.size(), 0);

        Results joinedData = performTwoTableJOIN(MONGO_TEST_TABLE, HIVE_TEST_TABLE);

        assertEquals(hiveTblData.size() * mongoTblData.size(), joinedData.size());
    }
}
