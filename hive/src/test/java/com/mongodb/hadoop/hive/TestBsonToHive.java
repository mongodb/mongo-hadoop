package com.mongodb.hadoop.hive;

import com.mongodb.hadoop.hive.output.HiveBSONFileOutputFormat;
import com.mongodb.hadoop.mapred.BSONFileInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestBsonToHive extends HiveTest {
    @Before
    public void setUp() {
        loadDataIntoBSONHiveTable(true);
        createMongoDBHiveTable(false);
        createEmptyHDFSHiveTable();
    }

    @After
    public void tearDown() {
        dropTable(BSON_TEST_TABLE);
        dropTable(MONGO_TEST_TABLE);
        dropTable(HIVE_TEST_TABLE);
    }


    private Results loadDataIntoBSONHiveTable(final boolean withLocation) {
        dropTable(BSON_TEST_TABLE);
        loadIntoHDFS(getPath("users.bson"), BSON_HDFS_TEST_PATH);

        String cmd = format("CREATE TABLE %s %s\n"
                            + "ROW FORMAT SERDE '%s'\n"
                            + "STORED AS INPUTFORMAT '%s'\n"
                            + "OUTPUTFORMAT '%s'",
                            BSON_TEST_TABLE, TEST_SCHEMA, BSONSerDe.class.getName(), BSONFileInputFormat.class.getName(),
                            HiveBSONFileOutputFormat.class.getName()
                           );
        if (withLocation) {
            cmd += format("\nLOCATION '%s'", BSON_HDFS_TEST_PATH);
        }
        return execute(cmd);
    }

    private void loadIntoHDFS(final String localPath, final String hdfsPath) {
        try {
            new ProcessExecutor(HADOOP_HOME + "/bin/hadoop", "fs", "-mkdir", hdfsPath)
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
        testTransfer(BSON_TEST_TABLE, MONGO_TEST_TABLE);
    }

    @Test
    public void testSameDataHDFSAndBSONHiveTables() {
        testTransfer(BSON_TEST_TABLE, MONGO_TEST_TABLE);
    }

    private void testTransfer(final String from, final String to) {
        transferData(from, to);

        Results toData = getAllDataFromTable(to);
        Results fromData = getAllDataFromTable(from);

        assertNotEquals(fromData.size(), 0);
        assertEquals(toData, fromData);
    }
}
