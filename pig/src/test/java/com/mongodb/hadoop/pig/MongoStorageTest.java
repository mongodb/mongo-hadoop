package com.mongodb.hadoop.pig;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MongoStorageTest {
    @Test
    public void testMap() throws Exception {
        MongoStorage ms = new MongoStorage();
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString("m:map[]"));

        Map<String, Object> val = new HashMap<String, Object>();
        val.put("f1", 1);
        val.put("f2", "2");

        ms.writeField(builder, schema.getFields()[0], val);

        DBObject out = builder.get();

        Set<String> outKeySet = out.keySet();

        assertEquals(2, outKeySet.size());
        assertEquals(1, out.get("f1"));
        assertEquals("2", out.get("f2"));
    }

}
