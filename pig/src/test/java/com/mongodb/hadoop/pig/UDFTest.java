package com.mongodb.hadoop.pig;

import com.mongodb.DBRef;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.pig.tools.parameters.ParseException;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UDFTest {
    private static final MongoClient CLIENT = new MongoClient(
      new MongoClientURI("mongodb://localhost:27017/mongo_hadoop"));
    private static final MongoCollection<Document> INPUT_COLLECTION =
      CLIENT.getDatabase("mongo_hadoop").getCollection("udftest.input");
    private static final MongoCollection<Document> OUTPUT_COLLECTION =
      CLIENT.getDatabase("mongo_hadoop").getCollection("udftest.output");
    private static List<Document> insertedDocuments;

    @Before
    public void setUp() {
        OUTPUT_COLLECTION.drop();
    }

    @BeforeClass
    public static void setUpClass() {
        INPUT_COLLECTION.drop();
        insertedDocuments = new ArrayList<Document>(100);
        for (int i = 0; i < 100; ++i) {
            ObjectId id = new ObjectId();
            insertedDocuments.add(
              new Document("_id", id)
                .append("minkey", new MinKey())
                .append("maxkey", new MaxKey())
                .append("dbref", new DBRef("othercollection", new ObjectId()))
                .append("binary", new Binary(new byte[]{1, 2, 3, 4, 5}))
                .append("oidBytes", new Binary(id.toByteArray())));
        }
        INPUT_COLLECTION.insertMany(insertedDocuments);
    }

    @AfterClass
    public static void tearDownClass() {
        INPUT_COLLECTION.drop();
        OUTPUT_COLLECTION.drop();
    }

    @Test
    public void testAsObjectId() throws IOException, ParseException {
        PigTest.runScript("/pig/toobjectid.pig");

        assertEquals(insertedDocuments.size(), OUTPUT_COLLECTION.count());
        Iterator<Document> it = insertedDocuments.iterator();
        for (Document outputDoc : OUTPUT_COLLECTION.find()) {
            ObjectId expectedId = it.next().getObjectId("_id");
            assertEquals(expectedId, outputDoc.get("_id"));
            assertEquals(expectedId, outputDoc.get("otherid"));
        }
    }

    @Test
    public void testAsBinary() throws IOException, ParseException {
        PigTest.runScript("/pig/tobinary.pig");

        for (Document doc : OUTPUT_COLLECTION.find()) {
            Object binary = doc.get("binary");
            assertTrue(binary instanceof Binary);
            assertArrayEquals(
              new byte[]{1, 2, 3, 4, 5},
              ((Binary) binary).getData());
        }
    }

    @Test
    public void testAsDBRef() throws IOException, ParseException {
        PigTest.runScript("/pig/todbref.pig");

        assertEquals(insertedDocuments.size(), OUTPUT_COLLECTION.count());
        Iterator<Document> it = insertedDocuments.iterator();
        for (Document outputDoc : OUTPUT_COLLECTION.find()) {
            assertEquals(it.next().get("dbref"), outputDoc.get("dbref"));
        }
    }

    @Test
    public void testMinMaxKey() throws IOException, ParseException {
        PigTest.runScript("/pig/genminmaxkeys.pig");

        for (Document doc : OUTPUT_COLLECTION.find()) {
            assertTrue(doc.get("newMin") instanceof MinKey);
            assertTrue(doc.get("newMax") instanceof MaxKey);
        }
    }

    @Test
    public void testObjectIdToSeconds() throws IOException, ParseException {
        PigTest.runScript("/pig/oidtoseconds.pig");

        assertEquals(insertedDocuments.size(), OUTPUT_COLLECTION.count());
        Iterator<Document> it = insertedDocuments.iterator();
        for (Document outputDoc : OUTPUT_COLLECTION.find()) {
            int seconds = outputDoc.getInteger("seconds");
            int seconds2 = outputDoc.getInteger("seconds2");
            int expectedSeconds = it.next().getObjectId("_id").getTimestamp();

            assertEquals(expectedSeconds, seconds);
            assertEquals(expectedSeconds, seconds2);
        }
    }

    @Test
    public void testUDFsSchemaless() throws IOException, ParseException {
        // Test that one of our UDFs can work without any schemas being
        // specified. This mostly tests that BSONStorage can infer the type
        // correctly.
        PigTest.runScript("/pig/udfschemaless.pig");

        assertEquals(insertedDocuments.size(), OUTPUT_COLLECTION.count());
        Iterator<Document> it = insertedDocuments.iterator();
        for (Document doc : OUTPUT_COLLECTION.find()) {
            // We don't know what Pig will call the fields that aren't "_id".
            ObjectId expectedId = it.next().getObjectId("_id");
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                // _id field contains a different ObjectId than the one we're
                // interested in.
                if ("_id".equals(entry.getKey())) {
                    continue;
                }
                assertEquals(expectedId, entry.getValue());
            }
        }
    }
}
