package com.mongodb.hadoop.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.pig.ResourceSchema;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;


public class BSONStorageTest {
    TupleFactory tf = TupleFactory.getInstance();
    
    @Test
    public void testNullMap() throws Exception {
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString("m:map[]"));

        assertNull(BSONStorage.getTypeForBSON(null, schema.getFields()[0], null));
    }
    
    @SuppressWarnings("rawtypes")
    @Test
    public void testEmbeddedObject() throws Exception {
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString("t:tuple(b:bytearray, it:tuple(c:chararray))"));

        Tuple innerTuple = tf.newTuple(1);
        innerTuple.set(0, "char");
        
        Tuple outerTuple = tf.newTuple(2);
        outerTuple.set(0, new DataByteArray("byte"));
        outerTuple.set(1, innerTuple);
        
        Map resultTuple = (Map) BSONStorage.getTypeForBSON(outerTuple, schema.getFields()[0], null);
        assertEquals(2, resultTuple.keySet().size());
        assertEquals("byte", resultTuple.get("b"));
        
        Map resultInnerTuple = (Map) resultTuple.get("it");
        assertEquals(1, resultInnerTuple.size());
        assertEquals("char", resultInnerTuple.get("c"));
    }
    
    @SuppressWarnings("rawtypes")
    @Test
    public void testEmbeddedBag() throws Exception {
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString("t:(group:bytearray, joined:bag{t:(clean_searches::user_id, clean_searches::timestamp)})"));

        DataByteArray group = new DataByteArray("bytey");
        
        Tuple innerTuple = tf.newTuple(2);
        innerTuple.set(0, new DataByteArray("user_id"));
        innerTuple.set(1, new DataByteArray("timestamp"));
        
        DataBag b = BagFactory.getInstance().newDefaultBag(Arrays.asList(innerTuple));
        
        Tuple outerTuple = tf.newTuple(2);
        outerTuple.set(0, group);
        outerTuple.set(1, b);
        
        Map resultTuple = (Map) BSONStorage.getTypeForBSON(outerTuple, schema.getFields()[0], null);
        assertEquals(2, resultTuple.keySet().size());
        assertEquals("bytey", resultTuple.get("group"));
        
        ArrayList resultBag = (ArrayList) resultTuple.get("joined");
        assertEquals(1, resultBag.size());
        assertEquals("user_id", ((Map)resultBag.get(0)).get("clean_searches::user_id"));
    }
}
