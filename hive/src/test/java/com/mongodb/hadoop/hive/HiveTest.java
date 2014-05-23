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
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.StartedProcess;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;

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
    private static HiveClient client;
    private MongoClientURI mongoTestURI;
    private MongoClient mongoClient;
    private static StartedProcess hiveServer;

    public HiveTest() {
        mongoTestURI = authCheck(new MongoClientURIBuilder()
                                     .collection("mongo_hadoop", MONGO_TEST_COLLECTION)
                                ).build();

    }

    @BeforeClass
    public static void setupHive() throws TException, IOException, InterruptedException {
        HashMap<String, String> env = new HashMap<String, String>(System.getenv());
        env.put("HADOOP_HOME", HADOOP_HOME);
        final ProcessExecutor executor = new ProcessExecutor(HIVE_HOME + "/bin/hive", "--service", "hiveserver")
                                             .environment(env)
                                             .redirectOutput(new FileOutputStream(new File(new File(HIVE_HOME), "hiveserver.log")))
                                             .destroyOnExit();
        hiveServer = executor.start();
        Thread.sleep(5000);

        TSocket transport = new TSocket("127.0.0.1", 10000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        client = new HiveClient(protocol);
        transport.open();

    }

    @AfterClass
    public static void tearDownHive() throws TException {
        if (client != null) {
            client.shutdown();
        }
        if (hiveServer != null) {
            hiveServer.future().cancel(true);
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

    protected void createHDFSHiveTable(final String name, final String schema, final String delimiter, final String type) {
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
            LOG.error(e.getMessage(), e);
        }
        return results;
    }

    protected Results performTwoTableJOIN(final String firstTable, final String secondTable) {
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
        execute(format("LOAD DATA LOCAL INPATH '%s'\n"
                       + "INTO TABLE %s", getPath("test_data.txt"), HIVE_TEST_TABLE));
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