package com.mongodb.hadoop.streaming.io;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.streaming.PipeMapRed;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongoUpdateOutputReaderTest {
    private BSONWritable bsonWritable = new BSONWritable();

    private DataInput inputFromBSONObject(final BSONObject object)
      throws IOException {
        PipedOutputStream outputStream = new PipedOutputStream();
        PipedInputStream inputStream = new PipedInputStream(outputStream);
        bsonWritable.setDoc(object);
        bsonWritable.write(new DataOutputStream(outputStream));
        return new DataInputStream(inputStream);
    }

    @Test
    public void testNoUpdate() throws IOException {
        // Test document, does not describe an update.
        DBObject notAnUpdate = new BasicDBObject("_id", 42);

        PipeMapRed pipeMapRed = mock(PipeMapRed.class);
        when(pipeMapRed.getClientInput()).thenReturn(
          inputFromBSONObject(notAnUpdate));

        MongoUpdateOutputReader reader = new MongoUpdateOutputReader();
        reader.initialize(pipeMapRed);
        assertTrue(reader.readKeyValue());
        assertEquals(notAnUpdate, reader.getCurrentValue().getQuery());
    }

    @Test
    public void testUpdate() throws IOException {
        BasicBSONObject query = new BasicDBObject("i", 42);
        BasicBSONObject modifiers = new BasicDBObject("$set",
          new BasicDBObject("a", "b"));
        DBObject update = new BasicDBObjectBuilder()
          .add("_id", query)
          .add("modifiers", modifiers)
          .push("options").add("multi", true).add("upsert", false).pop().get();
        MongoUpdateWritable muw = new MongoUpdateWritable(
          query, modifiers, false, true, false);

        PipeMapRed pipeMapRed = mock(PipeMapRed.class);
        when(pipeMapRed.getClientInput()).thenReturn(
          inputFromBSONObject(update));

        MongoUpdateOutputReader reader = new MongoUpdateOutputReader();
        reader.initialize(pipeMapRed);
        assertTrue(reader.readKeyValue());
        assertEquals(muw, reader.getCurrentValue());
    }
}
