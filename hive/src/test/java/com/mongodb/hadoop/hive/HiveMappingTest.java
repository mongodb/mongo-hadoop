package com.mongodb.hadoop.hive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.hive.output.HiveBSONFileOutputFormat;
import com.mongodb.hadoop.mapred.BSONFileInputFormat;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class HiveMappingTest extends HiveTest {

    @Test
    public void nestedObjects() {
        DBCollection collection = getCollection("hive_addresses");
        collection.drop();
        dropTable("hive_addresses");


        collection.insert(user(1, "Jim", "Beam", "Clermont", "KY"));
        collection.insert(user(2, "Don", "Draper", "New York", "NY"));
        collection.insert(user(3, "John", "Elway", "Denver", "CO"));

        MongoClientURI uri = authCheck(new MongoClientURIBuilder()
                                           .collection("mongo_hadoop", collection.getName())
                                      ).build();

        ColumnMapping map = new ColumnMapping()
                                .map("id", "_id")
                                .map("firstName", "firstName")
                                .map("lastName", "lastName")
                                .map("city", "address.city")
                                .map("state", "address.state");

        //, lastName STRING
        execute(format("CREATE TABLE hive_addresses (id INT, firstName STRING, lastName STRING, city STRING, state STRING)\n"
                       + "STORED BY '%s'\n"
                       + "WITH SERDEPROPERTIES('mongo.columns.mapping'='%s')\n"
                       + "TBLPROPERTIES ('mongo.uri'='%s')",
                       MongoStorageHandler.class.getName(), map, uri
                      ));
        Results execute = execute("SELECT * from hive_addresses");
        assertEquals("KY", execute.getRow(0).get("state"));
        assertEquals("Don", execute.getRow(1).get("firstname"));
        assertEquals("Denver", execute.getRow(2).get("city"));
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

    public void loadMailBson() {
        String name = "messages";
        DBCollection collection = getCollection(name);
        collection.drop();
        dropTable(name);

        loadIntoHDFS("examples/data/dump/enron_mail/messages.bson", "/user/hive/warehouse/enron");
        
        ColumnMapping map = new ColumnMapping()
                                .map("id", "_id")
                                .map("subFolder", "subFolder");
        MongoClientURI uri = authCheck(new MongoClientURIBuilder()
                                           .collection("mongo_hadoop", collection.getName())
                                      ).build();
        execute(format("CREATE EXTERNAL TABLE %s (body STRING, subFolder STRING, mailbox STRING, filename STRING)\n"
                       + "ROW FORMAT SERDE '%s'\n"
                       + "WITH SERDEPROPERTIES('mongo.columns.mapping'='%s')\n"
                       + "STORED AS INPUTFORMAT '%s'\n"
                       + "OUTPUTFORMAT '%s'\n"
                       + "LOCATION '%s'", name, BSONSerDe.class.getName(), map, BSONFileInputFormat.class.getName(),
                       HiveBSONFileOutputFormat.class.getName(), "/user/hive/warehouse/enron/"
                      ));
        //        execute(format("LOAD DATA LOCAL INPATH '%s' INTO TABLE %s",
        //                       new File(PROJECT_HOME, "examples/data/dump/enron_mail/messages.bson").getAbsolutePath(), 
        // "enron_messages"));

        Results execute = execute(String.format("SELECT COUNT(*) from %s", name));
    }


    private static class ColumnMapping {
        private final Map<String, String> columns = new LinkedHashMap<String, String>();

        public ColumnMapping map(final String hiveColumn, final String mongoField) {
            columns.put("\"" + hiveColumn + "\"", "\"" + mongoField + "\"");
            return this;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (Entry<String, String> entry : columns.entrySet()) {
                if (builder.length() != 0) {
                    builder.append(", ");
                }
                builder.append(format("%s : %s", entry.getKey(), entry.getValue()));
            }
            return "{ " + builder + " }";
        }
    }
}
