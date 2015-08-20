package com.mongodb.hadoop.hive;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class TablePropertiesTest extends HiveTest {

    private MongoCollection<Document> collection;

    @Before
    public void setUp() {
        MongoClientURI clientURI = new MongoClientURI(
          "mongodb://localhost:27017/mongo_hadoop.tabletest");
        MongoClient client = new MongoClient(clientURI);

        // Seed some documents into MongoDB.
        collection = client
          .getDatabase(clientURI.getDatabase())
          .getCollection(clientURI.getCollection());
        ArrayList<Document> documents = new ArrayList<Document>(1000);
        for (int i = 0; i < 1000; ++i) {
            documents.add(new Document("i", i));
        }
        collection.insertMany(documents);

        // Make sure table doesn't exist already.
        dropTable("props_file_test");
    }

    @After
    public void tearDown() {
        // Tear down collection.
        collection.drop();

        // Drop Hive table.
        dropTable("props_file_test");
    }

    @Test
    public void testPropertiesFile() throws SQLException {
        // Create the table.
        execute(
          "CREATE TABLE props_file_test"
            + " (id STRING, i INT)"
            + " STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'"
            + " WITH SERDEPROPERTIES('mongo.columns.mapping'='{\"id\":\"_id\"}')"
            + " TBLPROPERTIES('mongo.properties.path'='"
            + getPath("hivetable.properties") + "')");

        // Read and write some data through the table.
        Results results = query("SELECT i FROM props_file_test WHERE i >= 20");
        assertEquals(490, results.size());

        execute(
          "INSERT INTO props_file_test VALUES ('55d5005b6e32ab5664606195', 42)");
        assertEquals(2, collection.count(new Document("i", 42)));
    }
}
