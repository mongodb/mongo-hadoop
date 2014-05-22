package com.mongodb.hadoop.hive;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestHDFSToMongoDBWithOptions extends HiveTest {
    @Before
    public void setUp() {
        loadDataIntoHDFSHiveTable();
        loadDataIntoMongoDBHiveTable(true);
    }

    @After
    public void tearDown() {
        dropTable(MONGO_TEST_COLLECTION);
        dropTable(HIVE_TEST_TABLE);
    }

    @Test
    public void testMongoMapping() {
        DBObject doc = getCollection().findOne();
        String[] propsSplit = SERDE_PROPERTIES.split("=");

        int propsSplitLen = propsSplit.length;
        assertEquals(propsSplitLen % 2, 0);

        // now read in the 'mongo.columns.mapping' mapping
        String colsMap = null;
        for (int i = 0; i < propsSplit.length && colsMap == null; i++) {
            final String entry = propsSplit[i];
            if (entry.toLowerCase().equals("'mongo.columns.mapping'") && i - 1 < propsSplitLen) {
                colsMap = propsSplit[i + 1];
            }
        }

        assertNotNull(colsMap);
        // first remove '' around colsMap
        colsMap = colsMap.substring(1, colsMap.length() - 1);
        Set<String> docKeys = doc.keySet();

        for (String s : ((Map<String, String>) JSON.parse(colsMap)).values()) {
            assertTrue(docKeys.contains(s));
        }
    }

    @Test
    public void testCountSameTable() {
        Results hiveData = getAllDataFromTable(HIVE_TEST_TABLE);
        Results mongoData = getAllDataFromTable(MONGO_TEST_COLLECTION);
        assertNotEquals(hiveData.size(), 0);
        assertNotEquals(mongoData.size(), 0);

        assertEquals(hiveData, mongoData);
    }
}
