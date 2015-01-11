package com.mongodb.hadoop.io;

import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoUpdateWritableTest {
    @Test
    public void testSerialization() throws Exception {
        BasicDBObject query = new BasicDBObject("_id", new ObjectId());
        BasicDBObject modifiers = new BasicDBObject("$set", new BasicDBObject("foo", "bar"));
        MongoUpdateWritable writable = new MongoUpdateWritable(query, modifiers, true, false);


        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        writable.write(out);

        baos.flush();
        byte[] serializedBytes = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(serializedBytes);
        DataInputStream in = new DataInputStream(bais);

        writable = new MongoUpdateWritable(null, null, false, true);

        writable.readFields(in);

        assertEquals(query, writable.getQuery());
        assertEquals(modifiers, writable.getModifiers());
        assertTrue(writable.isUpsert());
        assertFalse(writable.isMultiUpdate());
    }
}
