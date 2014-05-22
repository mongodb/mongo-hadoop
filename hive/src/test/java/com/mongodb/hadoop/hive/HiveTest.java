package com.mongodb.hadoop.hive;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.testutils.MongoClientURIBuilder;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import static java.lang.String.format;

public class HiveTest extends BaseHadoopTest {
    public static final String HIVE_TEST_TABLE = "hive_test";
    public static final String BSON_TEST_TABLE = "bson_test";
    public static final String MONGO_TEST_TABLE = "mongo_test";
    public static final String MONGO_TEST_COLLECTION = "hive_test";
    public static final String TEST_SCHEMA = "(id INT, name STRING, age INT)";
    public static final String HIVE_TABLE_TYPE = "textfile";
    public static final String SERDE_PROPERTIES = "'mongo.columns.mapping'='{\"id\":\"_id\"}'";
    public static final String BSON_HDFS_TEST_PATH = "/user/hive/warehouse/bson_test_files/";

    private static final Logger LOG = LoggerFactory.getLogger(HiveTest.class);
    protected static HiveClient client;
    protected MongoClientURI mongoTestURI;
    private MongoClient mongoClient;

    public HiveTest() {
        mongoTestURI = authCheck(new MongoClientURIBuilder()
                                     .collection("mongo_hadoop", MONGO_TEST_COLLECTION)
                                ).build();

    }

    @BeforeClass
    public static void openClient() throws TException, IOException {
        TSocket transport = new TSocket("127.0.0.1", 10000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        client = new HiveClient(protocol);
        transport.open();
    }

    @AfterClass
    public static void closeClient() throws TException {
        if (client != null) {
            client.shutdown();
        }
    }

    protected MongoClient getMongoClient() {
        if (mongoClient == null) {
            try {
                mongoClient = new MongoClient(getInputUri());
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return mongoClient;
    }

    @Override
    protected MongoClientURI getInputUri() {
        return authCheck(new MongoClientURIBuilder()
                             .collection("mongo_hadoop", MONGO_TEST_COLLECTION)).build();
    }

    protected void createHDFSHiveTable(String name, String schema, String delimiter, String type) {
        execute(format("CREATE TABLE %s %s\n"
                       + "ROW FORMAT DELIMITED\n"
                       + "FIELDS TERMINATED BY '%s'\n"
                       + "STORED AS %s", name, schema, delimiter, type));
    }

    protected Results dropTable(final String tblName) {
        return execute(format("DROP TABLE %s", tblName));
    }

    protected Results execute(final String command) {
        Results results = new Results();
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info(format("Executing Hive command: %s", command.replace("\n", "\n\t")));
            }
            client.execute(command);
            results.process(client);
        } catch (Exception e) {
            results.process(e);
            e.printStackTrace();
        }
        return results;
    }

    protected Results performTwoTableJOIN(String firstTable, String secondTable) {
        return getAllDataFromTable(format("%s JOIN %s", firstTable, secondTable));
    }

    protected String getPath(final String resource) {
        try {
            return new File(getClass().getResource("/" + resource).toURI()).getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected Results getAllDataFromTable(final String table) {
        return execute("SELECT * FROM " + table);
    }

    protected void loadDataIntoHDFSHiveTable() {
        createEmptyHDFSHiveTable();
        execute(format("LOAD DATA LOCAL INPATH '%s'\n" +
                       "INTO TABLE %s", getPath("test_data.txt"), HIVE_TEST_TABLE));
    }

    public void createEmptyHDFSHiveTable() {
        dropTable(HIVE_TEST_TABLE);
        createHDFSHiveTable(HIVE_TEST_TABLE, TEST_SCHEMA, "\\t", HIVE_TABLE_TYPE);
    }

    protected void loadDataIntoMongoDBHiveTable(final boolean withSerDeProps) {
        createMongoDBHiveTable(withSerDeProps);
        transferData(HIVE_TEST_TABLE, MONGO_TEST_TABLE);
    }

    public void createMongoDBHiveTable(final boolean withSerDeProps) {
        dropTable(MONGO_TEST_TABLE);
        execute(format("CREATE TABLE %s %s\n"
                       + "STORED BY '%s'\n"
                       + "WITH SERDEPROPERTIES(%s)\n"
                       + "TBLPROPERTIES ('mongo.uri'='%s')", MONGO_TEST_TABLE, TEST_SCHEMA
                          , MongoStorageHandler.class.getName(),
                       withSerDeProps ? SERDE_PROPERTIES : "''=''",
                       mongoTestURI
                      ));
    }

    protected void transferData(final String from, final String to) {
        execute(format("INSERT OVERWRITE TABLE %s "
                       + "SELECT * FROM %s", to, from));
    }

    protected DBCollection getCollection() {
        return getMongoClient().getDB("mongo_hadoop").getCollection(MONGO_TEST_COLLECTION);
    }
}