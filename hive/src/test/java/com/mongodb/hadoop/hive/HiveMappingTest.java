package com.mongodb.hadoop.hive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static com.mongodb.hadoop.hive.BSONSerDe.MONGO_COLS;
import static com.mongodb.hadoop.hive.MongoStorageHandler.MONGO_URI;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HiveMappingTest extends HiveTest {

    private static final Log LOG = LogFactory.getLog(HiveMappingTest.class);

    @Test
    public void nestedColumns() throws SQLException {
        DBCollection collection = getCollection("hive_addresses");
        collection.drop();
        dropTable("hive_addresses");

        collection.insert(user(1, "Jim", "Beam", "Clermont", "KY"));
        collection.insert(user(2, "Don", "Draper", "New York", "NY"));
        collection.insert(user(3, "John", "Elway", "Denver", "CO"));

        MongoClientURI uri = authCheck(
          new MongoClientURIBuilder()
            .collection("mongo_hadoop", collection.getName())
        ).build();

        Map<String, String> map = new HashMap<String, String>() {
            {
                put("id", "_id");
                put("firstName", "firstName");
                put("lastName", "lastName");
                put("place.municipality", "address.city");
                put("place.region", "address.state");
            }
        };

        execute(
          format(
            "CREATE TABLE hive_addresses "
              + "(id INT, firstName STRING, lastName STRING, "
              + "place STRUCT<municipality:STRING, region:STRING>)\n"
              + "STORED BY '%s'\n"
              + "WITH SERDEPROPERTIES('mongo.columns.mapping'='%s')\n"
              + "TBLPROPERTIES ('mongo.uri'='%s')",
            MongoStorageHandler.class.getName(),
            JSON.serialize(map),
            uri
          ));
        // Alias inner fields to avoid retrieving entire struct as a String.
        Results execute = query("SELECT place.municipality AS city, place.region AS state, firstname from hive_addresses");
        assertEquals("KY", execute.getRow(0).get("state"));
        assertEquals("Don", execute.getRow(1).get("firstname"));
        assertEquals("Denver", execute.getRow(2).get("city"));
    }

    @Test
    public void nestedObjects() throws SQLException {
        DBCollection collection = getCollection("hive_addresses");
        collection.drop();
        dropTable("hive_addresses");


        collection.insert(user(1, "Jim", "Beam", "Clermont", "KY"));
        collection.insert(user(2, "Don", "Draper", "New York", "NY"));
        collection.insert(user(3, "John", "Elway", "Denver", "CO"));

        MongoClientURI uri = authCheck(
          new MongoClientURIBuilder()
            .collection("mongo_hadoop", collection.getName())
        ).build();

        ColumnMapping map = new ColumnMapping()
                                .map("id", "_id", "INT")
                                .map("firstName", "firstName", "STRING")
                                .map("lastName", "lastName", "STRING")
                                .map("city", "address.city", "STRING")
                                .map("state", "address.state", "STRING");

        //, lastName STRING
        execute(
          format(
            "CREATE TABLE hive_addresses (id INT, firstName STRING, lastName STRING, city STRING, state STRING)\n"
              + "STORED BY '%s'\n"
              + "WITH SERDEPROPERTIES('mongo.columns.mapping'='%s')\n"
              + "TBLPROPERTIES ('mongo.uri'='%s')",
            MongoStorageHandler.class.getName(), map.toSerDePropertiesString(),
            uri
          ));
        Results execute = query("SELECT * from hive_addresses");
        assertEquals("KY", execute.getRow(0).get("state"));
        assertEquals("Don", execute.getRow(1).get("firstname"));
        assertEquals("Denver", execute.getRow(2).get("city"));
    }

    @Test
    public void queryBasedHiveTable() throws SQLException {
        String tableName = "filtered";
        DBCollection collection = getCollection(tableName);
        collection.drop();
        dropTable(tableName);

        int size = 1000;
        for (int i = 0; i < size; i++) {
            collection.insert(new BasicDBObject("_id", i)
                                  .append("intField", i % 10)
                                  .append("booleanField", i % 2 == 0)
                                  .append("stringField", "" + (i % 2 == 0)));
        }

        MongoClientURI uri = authCheck(new MongoClientURIBuilder()
                                           .collection("mongo_hadoop", collection.getName())
                                      ).build();

        ColumnMapping map = new ColumnMapping()
                                .map("id", "_id", "INT")
                                .map("ints", "intField", "INT")
                                .map("booleans", "booleanField", "BOOLEAN")
                                .map("strings", "stringField", "STRING");

        HiveTableBuilder builder = new HiveTableBuilder()
                                       .mapping(map)
                                       .name(tableName)
                                       .uri(uri)
                                       .tableProperty(MongoConfigUtil.INPUT_QUERY, "{_id : {\"$gte\" : 900 }}");
        execute(builder.toString());

        assertEquals(format("Should find %d items", size), collection.count(), size);
        Results execute = query(format("SELECT * from %s where id=1", tableName));
        assertTrue(execute.size() == 0);
        int expected = size - 900;
        assertEquals(format("Should find only %d items", expected),
                     query("SELECT count(*) as count from " + tableName).iterator().next().get(0), "" + expected);
    }

    private DBObject user(final int id, final String first, final String last, final String city, final String state) {
        return new BasicDBObject("_id", id)
                   .append("firstName", first)
                   .append("lastName", last)
                   .append("address",
                           new BasicDBObject("city", city)
                               .append("state", state)
                          );
    }

    private static class ColumnMapping {
        private final List<Column> columns = new ArrayList<Column>();

        public ColumnMapping map(final String hiveColumn, final String mongoField, final String hiveType) {
            columns.add(new Column(hiveColumn, mongoField, hiveType));
            return this;
        }

        public String toColumnDesc() {
            StringBuilder builder = new StringBuilder();
            for (Column entry : columns) {
                if (builder.length() != 0) {
                    builder.append(", ");
                }
                builder.append(format("%s %s", entry.hiveColumn, entry.hiveType));
            }
            return builder.toString();
        }

        public String toSerDePropertiesString() {
            StringBuilder builder = new StringBuilder();
            for (Column entry : columns) {
                if (builder.length() != 0) {
                    builder.append(", ");
                }
                builder.append(format("\"%s\" : \"%s\"", entry.hiveColumn, entry.mongoField));
            }
            return "{ " + builder + " }";
        }

        private static class Column {
            private final String hiveColumn;
            private final String mongoField;
            private final String hiveType;

            public Column(final String hiveColumn, final String mongoField, final String hiveType) {
                this.hiveColumn = hiveColumn;
                this.mongoField = mongoField;
                this.hiveType = hiveType;
            }
        }
    }

    private static class HiveTableBuilder {
        private String tableName;
        private ColumnMapping mapping;
        private String storageHandler = MongoStorageHandler.class.getName();
        private MongoClientURI uri;
        private Map<String, String> tableProperties = new TreeMap<String, String>();

        public HiveTableBuilder name(final String name) {
            tableName = name;
            return this;
        }

        public HiveTableBuilder mapping(final ColumnMapping mapping) {
            this.mapping = mapping;
            return this;
        }

        public HiveTableBuilder storageHandler(final String storageHandler) {
            this.storageHandler = storageHandler;
            return this;
        }

        public HiveTableBuilder uri(final MongoClientURI uri) {
            this.uri = uri;
            return this;
        }

        public HiveTableBuilder tableProperty(final String key, final String value) {
            tableProperties.put(key, value);
            return this;
        }

        public String toString() {
            storageHandler = MongoStorageHandler.class.getName();
            StringBuilder props = new StringBuilder();
            tableProperties.put(MONGO_URI, uri.toString());
            for (Entry<String, String> entry : tableProperties.entrySet()) {
                if (props.length() != 0) {
                    props.append(", \n\t\t");
                }
                props.append(format("'%s' = '%s'", entry.getKey(), entry.getValue()));
            }

            return format("CREATE TABLE %s (%s)\n"
                          + "STORED BY '%s'\n"
                          + "WITH SERDEPROPERTIES('%s' = '%s')\n"
                          + "TBLPROPERTIES (%s)", tableName, mapping.toColumnDesc(), storageHandler,
                          MONGO_COLS, mapping.toSerDePropertiesString(), props
                         );
        }
    }
}
