package com.mongodb.hadoop.hive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.MongoClientURIBuilder;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestHDFSToMongoDBTable extends HiveTest {

    private final String serdeProperties = "'mongo.columns.mapping'='{\"id\":\"_id\"}'";
    private MongoClientURI mongoTestURI;

    public TestHDFSToMongoDBTable() {
        mongoTestURI = authCheck(new MongoClientURIBuilder()
                                     .collection("mongo_hadoop", MONGO_TEST_COLLECTION)
                                ).build();
    }

    @Before
    public void setUp() {
        loadDataIntoHDFSHiveTable();
        loadDataIntoMongoDBHiveTable(false);
    }

    public void tearDown() {
        dropTable(MONGO_TEST_COLLECTION);
        dropTable(HIVE_TEST_TABLE);
    }

    public void createEmptyHDFSHiveTable() {
        dropTable(HIVE_TEST_TABLE);
        createHDFSHiveTable(HIVE_TEST_TABLE, TEST_SCHEMA, "\\t", HIVE_TABLE_TYPE);
    }

    public void createMongoDBHiveTable(final boolean withSerDeProps) {
        dropTable(MONGO_TEST_TABLE);
        execute(format("CREATE TABLE %s %s\n"
                       + "STORED BY '%s'\n"
                       + "WITH SERDEPROPERTIES(%s)\n"
                       + "TBLPROPERTIES ('mongo.uri'='%s')", MONGO_TEST_TABLE, TEST_SCHEMA
                          , MongoStorageHandler.class.getName(),
                       withSerDeProps ? serdeProperties : "''=''",
                       mongoTestURI
                      ));
    }

    private void loadDataIntoHDFSHiveTable() {
        createEmptyHDFSHiveTable();
        execute(format("LOAD DATA LOCAL INPATH '%s'\n" +
                       "INTO TABLE %s", getPath("test_data.txt"), HIVE_TEST_TABLE));
    }

    private void loadDataIntoMongoDBHiveTable(final boolean withSerDeProps) {
        createMongoDBHiveTable(withSerDeProps);
        execute(format("INSERT OVERWRITE TABLE %s "
                       + "SELECT * FROM %s", MONGO_TEST_TABLE, HIVE_TEST_TABLE));
    }


    private Results getAllDataFromTable(final String table) {
        return execute("SELECT * FROM " + table);
    }

    @Test
    public void testSameDataHDFSAndMongoHiveTables() {
        Results hivedata = getAllDataFromTable(HIVE_TEST_TABLE);
        Results mongoData = getAllDataFromTable(MONGO_TEST_COLLECTION);

        assertEquals(hivedata, mongoData);
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
            }else if (schema.getType().equals("string")) {
                toDelete.put(schema.getName(), t.get(i));
            }else{
                toDelete.put(schema.getName(),t.get(i));
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
        getMongoClient().getDB("mongo_hadoop").getCollection(MONGO_TEST_COLLECTION).remove(toDelete);
    }
/*
public void testDropReflectData() {
    mongoSchema, mongoTblData = Helpers.getAllDataFromTable(self.client, testMongoTblName)
    assertTrue(len(mongoTblData) > 0)

    #now, drop the collection
    Helpers.dropCollection(self.mongoc)

    mongoSchema, mongoTblData = Helpers.getAllDataFromTable(self.client, testMongoTblName)
    assertTrue(len(mongoTblData) == 0)
}
public void testJOINHDFSMongoDB() {
        mongoSchema, mongoTblData = Helpers.getAllDataFromTable(self.client, testMongoTblName)
        hiveSchema, hiveTblData = Helpers.getAllDataFromTable(self.client, testHiveTblName)
        assertTrue(len(hiveTblData) > 0)
        assertTrue(len(mongoTblData) > 0)

        joinedSchema, joinedData = Helpers.performTwoTableJOIN(self.client, testMongoTblName, testHiveTblName)

        assertTrue(len(joinedData) == len(hiveTblData) * len(mongoTblData))
}
*/
}
