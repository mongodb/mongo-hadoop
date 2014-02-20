package com.mongodb.hadoop.pig;

import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

import static org.junit.Assert.assertNull;


public class BSONStorageTest {
    @Test
    public void testNullMap() throws Exception {
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString("m:map[]"));

        assertNull(BSONStorage.getTypeForBSON(null, schema.getFields()[0], null));
    }
}
