package com.mongodb.hadoop.hive;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class HiveQueryTest extends HiveTest {

    private static MongoCollection<Document> coll;

    @Before
    public void setUp() {
        MongoClient client = new MongoClient("localhost:27017");
        coll = client.getDatabase("mongo_hadoop").getCollection("hive_query");
        for (int i = 0; i < 1000; ++i) {
            coll.insertOne(new Document("i", i).append("j", i % 5));
        }
    }

    @After
    public void tearDown() {
        coll.drop();
        dropTable("querytest");
    }

    @Test
    public void testQueryPushdown() throws SQLException {
        execute(
          "CREATE EXTERNAL TABLE querytest (id STRING, i INT, j INT) "
            + "STORED BY \"com.mongodb.hadoop.hive.MongoStorageHandler\" "
            + "WITH SERDEPROPERTIES(\"mongo.columns.mapping\"="
            + "'{\"id\":\"_id\"}') "
            + "TBLPROPERTIES(\"mongo.uri\"="
            + "\"mongodb://localhost:27017/mongo_hadoop.hive_query\")");
        Results results = query("SELECT * FROM querytest WHERE i > 20");
        assertEquals(979, results.size());
    }

    @Test
    public void testQueryPushdownWithQueryTable() throws SQLException {
        execute(
          "CREATE EXTERNAL TABLE querytest (id STRING, i INT, j INT) "
            + "STORED BY \"com.mongodb.hadoop.hive.MongoStorageHandler\" "
            + "WITH SERDEPROPERTIES(\"mongo.columns.mapping\"="
            + "'{\"id\":\"_id\"}') "
            + "TBLPROPERTIES(\"mongo.uri\"="
            + "\"mongodb://localhost:27017/mongo_hadoop.hive_query\","
            + "\"mongo.input.query\"='{\"j\":{\"$eq\":0}}')");
        Results results = query("SELECT * FROM querytest WHERE i > 20");
        assertEquals(195, results.size());

        results = query("SELECT * from querytest WHERE j > 2");
        assertEquals(0, results.size());
    }
}
