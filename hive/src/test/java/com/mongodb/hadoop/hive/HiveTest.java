package com.mongodb.hadoop.hive;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.testutils.MongoClientURIBuilder;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

public class HiveTest extends BaseHadoopTest {
    public static final String HIVE_TEST_TABLE = "hive_test";
    public static final String MONGO_TEST_TABLE = "mongo_test";
    public static final String MONGO_TEST_COLLECTION = "hive_test";
    public static final String TEST_SCHEMA = "(id INT, name STRING, age INT)";
    public static final String HIVE_TABLE_TYPE = "textfile";
    private static final Logger LOG = LoggerFactory.getLogger(HiveTest.class);

    protected static HiveClient client;
    private MongoClient mongoClient;

    @BeforeClass
    public static void startHive() throws TException, IOException {
        TSocket transport = new TSocket("127.0.0.1", 10000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        client = new HiveClient(protocol);
        transport.open();
    }

    @AfterClass
    public static void stopHive() throws TException {
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

    @Test
    public void simpleJob() throws Exception {
        dropTable("hive_test");
        createHDFSHiveTable(HiveTest.HIVE_TEST_TABLE, HiveTest.TEST_SCHEMA, "\\t",
                            HiveTest.HIVE_TABLE_TYPE);
        client.execute(format("LOAD DATA LOCAL INPATH '%s'\nINTO TABLE hive_test", getPath("test_data.txt")));
    }

    protected void createHDFSHiveTable(String name, String schema, String delimiter, String type) {
        execute(format("CREATE TABLE %s %s\n"
                       + "ROW FORMAT DELIMITED\n"
                       + "FIELDS TERMINATED BY '%s'\n"
                       + "STORED AS %s", name, schema, delimiter, type));
    }

    protected void dropTable(final String tblName) {
        execute(format("DROP TABLE %s", tblName));
    }

    protected Results execute(final String command) {
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info(format("Executing Hive command: %s", command));
            }
            client.execute(command);
            return new Results(client);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    protected String getPath(final String resource) {
        try {
            return new File(getClass().getResource("/" + resource).toURI()).getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static class Results implements Iterable<List<String>> {

        private List<FieldSchema> fields;
        private List<List<String>> data = new ArrayList<List<String>>();

        public Results(final HiveClient client) throws TException {
            Schema schema = client.getSchema();
            fields = schema.getFieldSchemas();
            List<String> strings = client.fetchAll();
            for (String string : strings) {
                data.add(Arrays.asList(string.split("\t")));
            }
        }

        public int size() {
            return data.size();
        }

        public List<String> get(final int i) {
            return data.get(i);
        }

        public List<FieldSchema> getFields() {
            return fields;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            for (FieldSchema fieldSchema : fields) {
                sb.append(format(" %15s   |", fieldSchema.getName()));
            }
            sb.append("\n");
            for (List<String> row : data) {
                for (String s1 : row) {
                    sb.append(format(" %-15s   |", s1.trim()));
                }
                sb.append("\n");
            }
            return sb.toString();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Results)) {
                return false;
            }

            final Results results = (Results) o;

            if (data != null ? !data.equals(results.data) : results.data != null) {
                return false;
            }
            if (fields != null ? !fields.equals(results.fields) : results.fields != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = fields != null ? fields.hashCode() : 0;
            result = 31 * result + (data != null ? data.hashCode() : 0);
            return result;
        }

        @Override
        public Iterator<List<String>> iterator() {
            return data.iterator();
        }
    }
}