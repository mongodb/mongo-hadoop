package com.mongodb.hadoop.hive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestHDFSToMongoDB extends HiveTest {

    @Before
    public void setUp() throws SQLException {
        loadDataIntoHDFSHiveTable();
        loadDataIntoMongoDBHiveTable(false);
    }

    @After
    public void tearDown() throws SQLException {
        dropTable(MONGO_BACKED_TABLE);
        dropTable(HDFS_BACKED_TABLE);
    }


    @Test
    public void testSameDataHDFSAndMongoHiveTables() {
        Results hiveData = getAllDataFromTable(HDFS_BACKED_TABLE);
        Results mongoData = getAllDataFromTable(MONGO_BACKED_TABLE);
        assertNotEquals(hiveData.size(), 0);
        assertNotEquals(mongoData.size(), 0);

        assertEquals(hiveData, mongoData);
    }

    @Test
    public void testDeleteReflectData() {
        Results results = getAllDataFromTable(MONGO_BACKED_TABLE);

        int size = results.size();
        assertTrue(size > 0);

        List<String> t = results.get(new Random().nextInt(size));
        DBObject toDelete = new BasicDBObject();
        int i = 0;
        for (Results.Field field : results.getFields()) {
            // add more types as necessary
            if (field.getType().equals("int")) {
                toDelete.put(field.getName(), Integer.valueOf(t.get(i)));
            } else if (field.getType().equals("string")) {
                toDelete.put(field.getName(), t.get(i));
            } else {
                toDelete.put(field.getName(), t.get(i));
            }
            i++;
        }

        deleteFromCollection(toDelete);

        // get data from table now that the first row has been removed
        Results newResults = getAllDataFromTable(MONGO_BACKED_TABLE);

        // now make sure that 'toDelete' doesn't exist anymore
        for (List<String> newResult : newResults) {
            assertNotEquals(newResult, t);
        }
    }

    private void deleteFromCollection(final DBObject toDelete) {
        getCollection(MONGO_COLLECTION).remove(toDelete);
    }

    @Test
    public void testDropReflectData() {
        assertTrue(getAllDataFromTable(MONGO_BACKED_TABLE).size() > 0);
        getCollection(MONGO_COLLECTION).drop();
        assertEquals(0, getAllDataFromTable(MONGO_BACKED_TABLE).size());
    }

    @Test
    public void testJOINHDFSMongoDB() {
        Results mongoTblData = getAllDataFromTable(MONGO_BACKED_TABLE);
        Results hiveTblData = getAllDataFromTable(HDFS_BACKED_TABLE);
        assertNotEquals(hiveTblData.size(), 0);
        assertNotEquals(mongoTblData.size(), 0);

        Results joinedData = performTwoTableJOIN(MONGO_BACKED_TABLE, HDFS_BACKED_TABLE);

        assertEquals(hiveTblData.size() * mongoTblData.size(), joinedData.size());
    }
}
