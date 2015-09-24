package com.mongodb.hadoop.hive;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.File;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class HiveTest extends BaseHadoopTest {
    public static final String HDFS_BACKED_TABLE = "hdfs_backed";
    public static final String BSON_BACKED_TABLE = "bson_backed";
    public static final String MONGO_BACKED_TABLE = "mongo_backed";
    public static final String MONGO_COLLECTION = "hive_accessible";
    public static final String TEST_SCHEMA = "(id INT, name STRING, age INT)";
    public static final String HIVE_TABLE_TYPE = "textfile";
    public static final String SERDE_PROPERTIES = "'mongo.columns.mapping'='{\"id\":\"_id\"}'";
    public static final String BSON_HDFS_TEST_PATH = "hdfs://localhost:8020/user/hive/warehouse/bson_test_files/";

    private static final Logger LOG = LoggerFactory.getLogger(HiveTest.class);
    private static Connection connection;
    private final MongoClientURI mongoTestURI;
    private MongoClient mongoClient;

    public HiveTest() {
        mongoTestURI = authCheck(new MongoClientURIBuilder()
                                     .collection("mongo_hadoop", MONGO_COLLECTION)
                                ).build();

    }

    @BeforeClass
    public static void setupHive() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection("jdbc:hive2://", "", "");
    }

    @AfterClass
    public static void tearDownHive() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    protected MongoClient getMongoClient() {
        if (mongoClient == null) {
            try {
                mongoClient = new MongoClient(getInputUri());
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return mongoClient;
    }

    protected MongoClientURI getInputUri() {
        return mongoTestURI;
    }

    protected void createHDFSHiveTable(
      final String name, final String schema,
      final String delimiter, final String type) throws SQLException {
        execute(
          format(
            "CREATE TABLE %s %s\n"
              + "ROW FORMAT DELIMITED\n"
              + "FIELDS TERMINATED BY '%s'\n"
              + "STORED AS %s", name, schema, delimiter, type));
    }

    protected void dropTable(final String tblName) {
        try {
            execute(format("DROP TABLE %s", tblName));
        } catch (SQLException e) {
            LOG.error("Could not drop table " + tblName + ": " + e.getMessage());
        }
    }

    protected void execute(final String command) throws SQLException {
        if (LOG.isInfoEnabled()) {
            LOG.info(format("Executing Hive command: %s",
                            command.replace("\n", "\n\t")));
        }
        Statement statement = connection.createStatement();
        statement.execute(command);
    }

    protected Results query(final String queryStr) {
        final Results results = new Results();
        final ResultSet resultSet;
        try {
            Statement statement = connection.createStatement();
            if (LOG.isInfoEnabled()) {
                LOG.info(format(
                           "Executing Hive query: %s",
                           queryStr.replace("\n", "\n\t")));
            }
            resultSet = statement.executeQuery(queryStr);
            results.process(resultSet);
        } catch (SQLException e) {
            LOG.error("SQL query <" + queryStr + "> threw SQLException; "
                    + "it may not have returned a ResultSet.", e);
        }
        return results;
    }

    protected Results performTwoTableJOIN(final String firstTable, final String secondTable) {
        return getAllDataFromTable(format("%s JOIN %s", firstTable, secondTable));
    }

    protected String getPath(final String resource) {
        try {
            return new File(getClass().getResource("/" + resource).toURI()).getAbsolutePath();
        } catch (final URISyntaxException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected Results getAllDataFromTable(final String table) {
        return query("SELECT * FROM " + table);
    }

    protected void loadDataIntoHDFSHiveTable() throws SQLException {
        createEmptyHDFSHiveTable();
        execute(
          format(
            "LOAD DATA LOCAL INPATH '%s'\n"
              + "INTO TABLE %s", getPath("test_data.txt"), HDFS_BACKED_TABLE));
    }

    public void createEmptyHDFSHiveTable() throws SQLException {
        dropTable(HDFS_BACKED_TABLE);
        createHDFSHiveTable(HDFS_BACKED_TABLE, TEST_SCHEMA, "\\t", HIVE_TABLE_TYPE);
    }

    protected void loadDataIntoMongoDBHiveTable(final boolean withSerDeProps)
      throws SQLException {
        createMongoBackedTable(withSerDeProps);
        transferData(HDFS_BACKED_TABLE, MONGO_BACKED_TABLE);
    }

    public void createMongoBackedTable(final boolean withSerDeProps)
      throws SQLException {
        dropTable(MONGO_BACKED_TABLE);
        final MongoClientURI uri = authCheck(new MongoClientURIBuilder()
                                           .collection("mongo_hadoop", MONGO_COLLECTION)
                                      ).build();
        execute(
          format(
            "CREATE TABLE %s %s\n"
              + "STORED BY '%s'\n"
              + (withSerDeProps ? format(
              "WITH SERDEPROPERTIES(%s)\n", SERDE_PROPERTIES) : "")
              + "TBLPROPERTIES ('mongo.uri'='%s')", MONGO_BACKED_TABLE,
            TEST_SCHEMA, MongoStorageHandler.class.getName(),
            uri
          ));
    }

    protected void transferData(final String from, final String to)
      throws SQLException {
        execute(
          format(
            "INSERT OVERWRITE TABLE %s "
              + "SELECT * FROM %s", to, from));
    }

    protected DBCollection getCollection(final String collection) {
        return getMongoClient().getDB("mongo_hadoop").getCollection(collection);
    }

    protected void loadIntoHDFS(final String localPath, final String hdfsPath) {
        try {
            new ProcessExecutor(HADOOP_HOME + "/bin/hadoop", "fs", "-mkdir", "-p", hdfsPath)
                .redirectOutput(System.out)
                .execute();
            new ProcessExecutor(HADOOP_HOME + "/bin/hadoop", "fs", "-put", localPath, hdfsPath)
                .directory(PROJECT_HOME)
                .redirectOutput(System.out)
                .execute();
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
