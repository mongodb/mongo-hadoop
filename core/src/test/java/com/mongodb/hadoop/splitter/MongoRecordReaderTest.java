package com.mongodb.hadoop.splitter;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.input.MongoRecordReader;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import org.bson.BasicBSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoRecordReaderTest {

    @Test
    public void testGetCurrentKey() throws Exception {
        MongoClient client = new MongoClient("localhost", 27017);
        MongoClientURI uri = new MongoClientURIBuilder()
                .collection("mongo_hadoop", "mongo_record_reader_test")
                .build();
        DBCollection collection = client.getDB(uri.getDatabase()).getCollection(uri.getCollection());
        collection.drop();
        BasicDBList colors = new BasicDBList(){
            {
                add(new BasicBSONObject("red", 255));
                add(new BasicBSONObject("blue", 255));
                add(new BasicBSONObject("green", 0));
            }
        };
        collection.insert(
                new BasicDBObject("_id", 0)
                        .append("address",
                                new BasicDBObject("street", "foo street"))
                        .append("colors", colors)
        );

        // Default case: "_id" is used as inputKey.
        MongoInputSplit split = new MongoInputSplit();
        split.setInputURI(uri);
        MongoRecordReader reader = new MongoRecordReader(split);
        assertTrue(reader.nextKeyValue());
        assertEquals(reader.getCurrentKey(), 0);

        // Use a nested field as inputKey.
        split = new MongoInputSplit();
        split.setInputURI(uri);
        split.setKeyField("address.street");
        reader = new MongoRecordReader(split);
        assertTrue(reader.nextKeyValue());
        assertEquals(reader.getCurrentKey(), "foo street");

        // Use a key within an array as the inputKey.
        split = new MongoInputSplit();
        split.setInputURI(uri);
        split.setKeyField("colors.1");
        reader = new MongoRecordReader(split);
        assertTrue(reader.nextKeyValue());
        assertEquals(reader.getCurrentKey(), new BasicBSONObject("blue", 255));
    }
}
