package com.mongodb.hadoop.hive;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.MongoClientURIBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class TestHDFSToMongoDBTable extends HiveTest {

    public static final String HIVE_TEST_TABLE = "hive_test";
    public static final String MONGO_TEST_TABLE = "mongo_test";
    public static final String MONGO_TEST_COLLECTION = "hive_test";
    public static final String TEST_SCHEMA = "(id INT, name STRING, age INT)";
    public static final String HIVE_TABLE_TYPE = "textfile";
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
    public void  testSameDataHDFSAndMongoHiveTables() {
        Results hivedata = getAllDataFromTable(HIVE_TEST_TABLE);
        Results mongoData = getAllDataFromTable(MONGO_TEST_COLLECTION);

        assertEquals(hivedata, mongoData);
    }

/*
    public void testDeleteReflectData() {
    mongoSchema, mongoTblData = Helpers.getAllDataFromTable(self.client, testMongoTblName)
    mongoSchema = mongoSchema.fieldSchemas

    l = len(mongoTblData)
    assertTrue(l > 0)

    t = mongoTblData[random.randint(0, l - 1)]
    toDelete = {}
    for i in range(len(mongoSchema)):
    #add more types as necessary
    if mongoSchema[i].type == "int":
    toDelete[mongoSchema[i].name] =int(t[i])
    elif mongoSchema[ i].type == "string":
    toDelete[mongoSchema[i].name] = str(t[i])
    else:
    toDelete[mongoSchema[i].name] = t[i]

    Helpers.deleteFromCollection(self.mongoc, toDelete)

    #get data from table now that the first row has been removed
    mongoSchema, mongoTblData = Helpers.getAllDataFromTable(self.client, testMongoTblName)

    #now make sure that 'toDelete' doesn 't exist anymore
    for line in mongoTblData:
    assertNotEqual(line, t)
}
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
