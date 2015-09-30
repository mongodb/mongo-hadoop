package com.mongodb.hadoop.pig;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import org.apache.pig.tools.parameters.ParseException;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class PigTest extends BaseHadoopTest {
    private static final MongoClientURI URI =
      new MongoClientURI("mongodb://localhost:27017/mongo_hadoop.pigtests");
    private MongoClient mongoClient;

    @Before
    public void setup() throws UnknownHostException {
        mongoClient = new MongoClient(URI);
        mongoClient.getDB("mongo_hadoop").dropDatabase();
    }

    @After
    public void tearDown() {
        mongoClient.getDB("mongo_hadoop").dropDatabase();
        mongoClient.close();
    }

    public void runMongoUpdateStorageTest(
      final String scriptName, final String[] expected)
      throws IOException, ParseException {
        runMongoUpdateStorageTest(scriptName, expected, "results");
    }

    public void runMongoUpdateStorageTest(
      final String scriptName, final String[] expected, final String alias)
      throws IOException, ParseException {
        org.apache.pig.pigunit.PigTest pigTest = new org.apache.pig.pigunit
          .PigTest(getClass().getResource(scriptName).getPath());

        // Let the STORE statement do its job so we can test MongoUpdateStorage.
        pigTest.unoverride("STORE");

        pigTest.assertOutput(alias, expected);
    }

    private boolean indexExists(
      final MongoCollection<Document> collection, final String indexName) {
        ListIndexesIterable<Document> indexes = collection.listIndexes();
        for (Document indexSpec : indexes) {
            String idxName = (String) indexSpec.get("name");
            if (idxName.equals(indexName)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void mongoUpdateStorage() throws IOException, ParseException {
        runMongoUpdateStorageTest(
          "/pig/update_simple_mus.pig",
          new String[]{
            "(Daniel,Alabi,([car#a],[car#b],[car#c],[car#a],[car#b],[car#c]))",
            "(Tolu,Alabi,([car#d],[car#e],[car#f],[car#d],[car#e],[car#f]))",
            "(Tinuke,Dada,([car#g],[car#g]))"
          }
        );
    }

    @Test
    public void mongoUpdateStorageMulti() throws IOException, ParseException {
        runMongoUpdateStorageTest(
          "/pig/update_age_alabis_mus.pig",
          new String[]{
            "(Daniel,Alabi,22.0)",
            "(Tolu,Alabi,24.0)",
            "(Tinuke,Dada,53.0)"
          }
        );
    }

    @Test
    public void testPigUUID() throws IOException, ParseException {
        UUID uuid = UUID.randomUUID();
        BasicDBObject doc = new BasicDBObject("uuid", uuid);
        mongoClient.getDB("mongo_hadoop")
          .getCollection("uuid_test").insert(doc);

        org.apache.pig.pigunit.PigTest test =
          new org.apache.pig.pigunit.PigTest(
            getClass().getResource("/pig/pig_uuid.pig").getPath());
        test.assertOutput(new String[]{"(" + uuid.toString() + ")"});
    }

    @Test
    public void testPigBSONOutput() throws IOException, ParseException {
        runMongoUpdateStorageTest(
          "/pig/bson_test.pig",
          new String[]{
            "(Daniel,Alabi,19.0)",
            "(Tolu,Alabi,21.0)",
            "(Tinuke,Dada,50.0)"
          },
          "persons_read"
        );
    }

    @Test
    public void testPigSchemaless() throws IOException, ParseException {
        // Seed data used by "schemaless.pig"
        MongoDatabase db = mongoClient.getDatabase("mongo_hadoop");
        List<Document> documents = new ArrayList<Document>(1000);
        for (int i = 0; i < 1000; ++i) {
            documents.add(new Document("_id", i));
        }
        db.getCollection("pig.schemaless").insertMany(documents);

        org.apache.pig.pigunit.PigTest test =
          new org.apache.pig.pigunit.PigTest(
            getClass().getResource("/pig/schemaless.pig").getPath());
        test.unoverride("STORE");
        test.runScript();

        assertEquals(1000, db.getCollection("pig.schemaless.out").count());
        assertNotNull(
          db.getCollection("pig.schemaless.out").find(
            new Document("_id", 999)).first());
    }

    @Test
    public void testPigSchemalessFromBSON() throws IOException, ParseException {
        runMongoUpdateStorageTest(
          "/pig/bson_schemaless.pig",
          new String[]{
            "(Daniel,Alabi,19.0)",
            "(Tolu,Alabi,21.0)",
            "(Tinuke,Dada,50.0)"
          }
        );
    }

    @Test
    public void testMongoStorageEnsureIndex()
      throws IOException, ParseException {
        org.apache.pig.pigunit.PigTest test =
          new org.apache.pig.pigunit.PigTest(
            getClass().getResource("/pig/ensure_index.pig").getPath());
        test.unoverride("STORE");
        test.runScript();

        MongoClient client = new MongoClient("localhost:27017");
        // There should be an index on the "last" field, ascending.
        MongoCollection<Document> coll = client.getDatabase("mongo_hadoop")
          .getCollection("ensure_indexes");
        assertTrue("Should have the index \"last_1\"",
          indexExists(coll, "last_1"));

        //  Drop the index.
        coll.dropIndex("last_1");

        // Run the second pig script, which ensures a different index.
        org.apache.pig.pigunit.PigTest secondTest =
          new org.apache.pig.pigunit.PigTest(
            getClass().getResource("/pig/ensure_index_2.pig").getPath());
        secondTest.unoverride("STORE");
        secondTest.runScript();

        assertTrue("Should have the index \"first_1\"",
          indexExists(coll, "first_1"));
        assertFalse("Should not have the index \"last_1\"",
          indexExists(coll, "last_1"));
    }
}
