package com.mongodb.hadoop.hive;

import com.jayway.awaitility.Awaitility;
import com.mongodb.hadoop.hive.output.HiveBSONFileOutputFormat;
import com.mongodb.hadoop.mapred.BSONFileInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.util.concurrent.Callable;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class TestBsonToHive extends HiveTest {
    @Before
    public void setUp() {
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


    private Results loadDataIntoBSONHiveTable(final boolean withLocation) {
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
        return execute(cmd);
    }

    private void loadIntoHDFS(final String localPath, final String hdfsPath) {
        try {
            new ProcessExecutor(HADOOP_HOME + "/bin/hadoop", "fs", "-mkdir", "-p", hdfsPath)
                .redirectOutput(System.out)
                .execute();
            new ProcessExecutor(HADOOP_HOME + "/bin/hadoop", "fs", "-put", localPath, hdfsPath)
                .redirectOutput(System.out)
                .execute();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Test
    public void testSameDataMongoAndBSONHiveTables() {
        testTransfer(BSON_BACKED_TABLE, MONGO_BACKED_TABLE);
    }

    @Test
    public void testSameDataHDFSAndBSONHiveTables() {
        testTransfer(BSON_BACKED_TABLE, MONGO_BACKED_TABLE);
    }

    private void testTransfer(final String from, final String to) {
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
