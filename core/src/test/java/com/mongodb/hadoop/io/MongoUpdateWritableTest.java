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
import static org.junit.Assert.assertNotEquals;

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

    @Test
    public void testEquals() {
        BasicDBObject query1 = new BasicDBObject("_id", new ObjectId());
        BasicDBObject query2 = new BasicDBObject("_id", new ObjectId());
        BasicDBObject modifiers1 = new BasicDBObject("$set", new
                BasicDBObject("foo", "bar"));
        BasicDBObject modifiers2 = new BasicDBObject("$set", new
                BasicDBObject("bar", "baz"));
        MongoUpdateWritable writable = new MongoUpdateWritable(query1,
                modifiers1, true, false);

        // Not equal because queries differ.
        MongoUpdateWritable diffQuery = new MongoUpdateWritable(query2,
                modifiers1, true, false);
        assertNotEquals(writable, diffQuery);

        // Not equal because modifiers differ.
        MongoUpdateWritable diffModifier = new MongoUpdateWritable(query1,
                modifiers2, true, false);
        assertNotEquals(writable, diffModifier);

        // Not equal because upsert flag differs.
        MongoUpdateWritable diffUpsert = new MongoUpdateWritable(query1,
                modifiers1, false, false);
        assertNotEquals(writable, diffUpsert);

        // Not equal because multi flag differs.
        MongoUpdateWritable diffMulti = new MongoUpdateWritable(query1,
                modifiers1, true, true);
        assertNotEquals(writable, diffMulti);

        MongoUpdateWritable same = new MongoUpdateWritable(query1, modifiers1,
                true, false);
        assertEquals(writable, same);
    }
}
