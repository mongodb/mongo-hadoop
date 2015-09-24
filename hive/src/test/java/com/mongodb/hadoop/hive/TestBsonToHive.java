package com.mongodb.hadoop.hive;

import com.jayway.awaitility.Awaitility;
import com.mongodb.hadoop.hive.output.HiveBSONFileOutputFormat;
import com.mongodb.hadoop.mapred.BSONFileInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.Callable;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class TestBsonToHive extends HiveTest {
    @Before
    public void setUp() throws SQLException {
        tearDown();
        loadDataIntoBSONHiveTable(true);
        createMongoBackedTable(false);
        createEmptyHDFSHiveTable();
    }

    @After
    public void tearDown() {
        dropTable(BSON_BACKED_TABLE);
        dropTable(MONGO_BACKED_TABLE);
        dropTable(HDFS_BACKED_TABLE);
    }


    private void loadDataIntoBSONHiveTable(final boolean withLocation)
      throws SQLException {
        loadIntoHDFS(getPath("users.bson"), BSON_HDFS_TEST_PATH);

        String cmd = format("CREATE TABLE %s %s\n"
                            + "ROW FORMAT SERDE '%s'\n"
                            + "STORED AS INPUTFORMAT '%s'\n"
                            + "OUTPUTFORMAT '%s'",
                            BSON_BACKED_TABLE, TEST_SCHEMA, BSONSerDe.class.getName(), BSONFileInputFormat.class.getName(),
                            HiveBSONFileOutputFormat.class.getName()
                           );
        if (withLocation) {
            cmd += format("\nLOCATION '%s'", BSON_HDFS_TEST_PATH);
        }
        execute(cmd);
    }

    @Test
    public void testSameDataMongoAndBSONHiveTables() throws SQLException {
        testTransfer(BSON_BACKED_TABLE, MONGO_BACKED_TABLE);
    }

    @Test
    public void testSameDataHDFSAndBSONHiveTables() throws SQLException {
        testTransfer(BSON_BACKED_TABLE, MONGO_BACKED_TABLE);
    }

    private void testTransfer(final String from, final String to)
      throws SQLException {
        transferData(from, to);
        Awaitility
            .await()
            .atMost(1, MINUTES)
            .pollDelay(1, MILLISECONDS)
            .pollInterval(3, SECONDS)
            .until(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return getAllDataFromTable(to).size() > 0;
                }
            });
        assertEquals(getAllDataFromTable(to), getAllDataFromTable(from));
    }
}
