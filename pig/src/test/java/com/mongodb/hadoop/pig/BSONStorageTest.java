package com.mongodb.hadoop.pig;

import static org.junit.Assert.assertNull;
import org.junit.Test;

import java.util.Map;

import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.Utils;


public class BSONStorageTest {
	@Test
    public void testGetTypeForBSON_map_null() throws Exception {
        BSONStorage bs = new BSONStorage();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString("m:map[]"));
        
        Map val = null;

        Object out = bs.getTypeForBSON(val, schema);
        
        assertNull(out);
    }
}
