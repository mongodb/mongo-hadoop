package com.mongodb.hadoop.pig;

import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNull;


public class BSONStorageTest {
    @Test
    public void testNullMap() throws Exception {
        BSONStorage bs = new BSONStorage();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString("m:map[]"));

        Map val = null;

        Object out = bs.getTypeForBSON(val, schema.getFields()[0], null);

        assertNull(out);
    }
}
