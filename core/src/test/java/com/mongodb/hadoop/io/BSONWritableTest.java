package com.mongodb.hadoop.io;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.bson.BasicBSONObject;
import org.junit.Test;

import java.util.Arrays;

import static com.mongodb.hadoop.io.BSONWritable.toBSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BSONWritableTest {

    @Test
    public void testToBSON() {
        assertEquals(null, toBSON(null));
        assertEquals(null, toBSON(NullWritable.get()));

        assertEquals("hello", toBSON(new Text("hello")));

        DBObject obj = new BasicDBObject("hello", "world");
        assertEquals(obj, toBSON(new BSONWritable(obj)));

        final BasicBSONObject bsonResult = new BasicBSONObject("one", 1);
        SortedMapWritable smw = new SortedMapWritable();
        smw.put(new Text("one"), new IntWritable(1));
        assertEquals(bsonResult, toBSON(smw));

        MapWritable mw = new MapWritable();
        mw.put(new Text("one"), new IntWritable(1));
        assertEquals(bsonResult, toBSON(mw));

        String[] expectedObjects = new String[] {"one", "two"};
        Writable[] writableObjects = new Writable[] {
          new Text("one"), new Text("two")
        };
        ArrayWritable aw = new ArrayWritable(Text.class, writableObjects);
        Object[] actual = (Object[]) toBSON(aw);
        assertTrue(Arrays.equals(expectedObjects, actual));

        assertEquals(false, toBSON(new BooleanWritable(false)));

        byte[] bytes = new byte[] {'0', '1', '2'};
        assertEquals(bytes, toBSON(new BytesWritable(bytes)));

        byte b = (byte) 'c';
        assertEquals(b, toBSON(new ByteWritable(b)));

        assertEquals(3.14159, toBSON(new DoubleWritable(3.14159)));

        assertEquals(3.14159f, toBSON(new FloatWritable(3.14159f)));

        assertEquals(42L, toBSON(new LongWritable(42L)));

        assertEquals(42, toBSON(new IntWritable(42)));

        // Catchall
        assertEquals("hi", toBSON("hi"));
    }
}
