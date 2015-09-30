package com.mongodb.hadoop.hive;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestHDFSToMongoDBWithOptions extends HiveTest {
    @Before
    public void setUp() throws SQLException {
        loadDataIntoHDFSHiveTable();
        loadDataIntoMongoDBHiveTable(true);
    }

    @After
    public void tearDown() throws SQLException {
        dropTable(MONGO_BACKED_TABLE);
        dropTable(HDFS_BACKED_TABLE);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMongoMapping() {
        DBObject doc = getCollection(MONGO_COLLECTION).findOne();
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
        Results hiveData = getAllDataFromTable(HDFS_BACKED_TABLE);
        Results mongoData = getAllDataFromTable(MONGO_BACKED_TABLE);
        assertNotEquals(hiveData.size(), 0);
        assertNotEquals(mongoData.size(), 0);

        assertEquals(hiveData, mongoData);
    }
}
