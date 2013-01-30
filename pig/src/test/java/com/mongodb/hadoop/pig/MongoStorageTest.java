package com.mongodb.hadoop.pig;

import junit.framework.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.pig.ResourceSchema;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;

import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

@SuppressWarnings( {"rawtypes", "unchecked"} )
public class MongoStorageTest {
    
    @Test
    public void mapWithSchema() throws Exception {
        MongoStorage ms = new MongoStorage();
        BasicDBObject obj = new BasicDBObject();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(
            "m: [int]")
        );
        
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 3);
        
        ms.writeField(obj, schema.getFields()[0], map);
        
        DBObject out = (DBObject) obj;
        Set<String> outKeySet = out.keySet();
        Assert.assertEquals(1, outKeySet.size());
        
        DBObject mOut = (DBObject) out.get("m");
        Assert.assertEquals(1, mOut.get("a"));
        Assert.assertEquals(2, mOut.get("b"));
        Assert.assertEquals(3, mOut.get("c"));
    }
    
    @Test
    public void mapToBytearrayWithoutSchema() throws Exception {
        MongoStorage ms = new MongoStorage();
        BasicDBObject obj = new BasicDBObject();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(
            "m: []")
        );
        
        Map<String, DataByteArray> map = new HashMap<String, DataByteArray>();
        map.put("a", new DataByteArray(new byte[] { 36, 37, 38 }));
        map.put("b", new DataByteArray(new byte[] { 40, 41, 42 }));
        
        ms.writeField(obj, schema.getFields()[0], map);
        
        DBObject out = (DBObject) obj;
        Set<String> outKeySet = out.keySet();
        Assert.assertEquals(1, outKeySet.size());
        
        DBObject mOut = (DBObject) out.get("m");
        Assert.assertEquals("$%&", mOut.get("a").toString());
        Assert.assertEquals("()*", mOut.get("b").toString());
    }
    
    @Test
    public void mapWithoutSchema() throws Exception {
        MongoStorage ms = new MongoStorage();
        BasicDBObject obj = new BasicDBObject();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(
            "m: map[]")
        );
        
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("i", new Integer(1));
        map.put("l", new Long(1000000000000L));
        map.put("f", new Float(3.14159f));
        map.put("d", new Double(3.14159265359));
        map.put("c", "aardvark");
        
        ms.writeField(obj, schema.getFields()[0], map);
        
        DBObject out = (DBObject) obj;
        Set<String> outKeySet = out.keySet();
        Assert.assertEquals(1, outKeySet.size());
        
        DBObject mOut = (DBObject) out.get("m");
        Assert.assertEquals(1, mOut.get("i"));
        Assert.assertEquals(1000000000000L, mOut.get("l"));
        Assert.assertEquals(3.14159f, mOut.get("f"));
        Assert.assertEquals(3.14159265359, mOut.get("d"));
        Assert.assertEquals("aardvark", mOut.get("c"));
    }

    @Test
    public void tuple() throws Exception {
        MongoStorage ms = new MongoStorage();
        BasicDBObject obj = new BasicDBObject();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(
            "t: (i: int, l: long, f: float, d: double, c: chararray, b: bytearray)")
        );

        TupleFactory tupleFactory = TupleFactory.getInstance();

        Tuple t = tupleFactory.newTuple(6);
        t.set(0, new Integer(1));
        t.set(1, new Long(1000000000000L));
        t.set(2, new Float(3.14159));
        t.set(3, new Double(3.14159265359));
        t.set(4, "aardvark");
        t.set(5, "beryllium");

        ms.writeField(obj, schema.getFields()[0], t);

        DBObject out = (DBObject) obj;
        Set<String> outKeySet = out.keySet();
        Assert.assertEquals(1, outKeySet.size());
        
        DBObject tOut = (DBObject) out.get("t");
        Assert.assertEquals(1, tOut.get("i"));
        Assert.assertEquals(1000000000000L, tOut.get("l"));
        Assert.assertEquals(3.14159f, tOut.get("f"));
        Assert.assertEquals(3.14159265359, tOut.get("d"));
        Assert.assertEquals("aardvark", tOut.get("c"));
        Assert.assertEquals("beryllium", tOut.get("b"));
    }
    
    @Test
    public void bag() throws Exception {
    	MongoStorage ms = new MongoStorage();
    	BasicDBObject obj = new BasicDBObject();
    	ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(
    	    "b: {t: (i: int)}"
    	));
    	
    	BagFactory bagFactory = BagFactory.getInstance();
    	TupleFactory tupleFactory = TupleFactory.getInstance();
    	
    	DataBag bag = bagFactory.newDefaultBag();
    	for (int i = 0; i < 3; i++) {
    	    Tuple t = tupleFactory.newTuple(1);
    	    t.set(0, new Integer(i));
    	    bag.add(t);
    	}
    	
    	ms.writeField(obj, schema.getFields()[0], bag);

        DBObject out = (DBObject) obj;
        Set<String> outKeySet = out.keySet();
        Assert.assertEquals(1, outKeySet.size());
        
        BasicDBList bagOut = (BasicDBList) out.get("b");
        for (int i = 0; i < 3; i++) {
            DBObject tOut = (DBObject) bagOut.get(i);
            Assert.assertEquals(i, tOut.get("i"));
        }
    }
    
    @Test
    public void complexMapWithSchema() throws Exception {
        MongoStorage ms = new MongoStorage();
        BasicDBObject obj = new BasicDBObject();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(
            "m: [(i: int, j: int)]")
        );
        
        TupleFactory tupleFactory = TupleFactory.getInstance();
        
        Map<String, Tuple> map = new HashMap<String, Tuple>();
        
        Tuple t = tupleFactory.newTuple(2);
        t.set(0, new Integer(1));
        t.set(1, new Integer(2));
        map.put("a", t);
        
        t = tupleFactory.newTuple(2);
        t.set(0, new Integer(3));
        t.set(1, new Integer(4));
        map.put("b", t);
        
        ms.writeField(obj, schema.getFields()[0], map);
        
        DBObject out = (DBObject) obj;
        Set<String> outKeySet = out.keySet();
        Assert.assertEquals(1, outKeySet.size());
        
        DBObject mOut = (DBObject) out.get("m");
        
        DBObject aOut = (DBObject) mOut.get("a");
        Assert.assertEquals(1, aOut.get("i"));
        Assert.assertEquals(2, aOut.get("j"));
        
        DBObject bOut = (DBObject) mOut.get("b");
        Assert.assertEquals(3, bOut.get("i"));
        Assert.assertEquals(4, bOut.get("j"));
    }
    
    @Test
    public void complexMapWithoutSchema() throws Exception {
        MongoStorage ms = new MongoStorage();
        BasicDBObject obj = new BasicDBObject();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(
            "m: map[]")
        );
        
        Map<String, Object> map = new HashMap<String, Object>();
        BagFactory bagFactory = BagFactory.getInstance();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        
        Map<String, Integer> nestedMap = new HashMap<String, Integer>();
        nestedMap.put("a", new Integer(1));
        nestedMap.put("b", new Integer(2));
        map.put("nm", nestedMap);
        
        Tuple t = tupleFactory.newTuple(2);
        t.set(0, new Integer(1));
        t.set(1, new Integer(2));
        map.put("nt", t);
        
        DataBag b = bagFactory.newDefaultBag();
        for (int i = 0; i < 3; i++) {
            t = tupleFactory.newTuple(1);
            t.set(0, new Integer(i));
            b.add(t);
        }
        map.put("nb", b);
        
        ms.writeField(obj, schema.getFields()[0], map);
        
        DBObject out = (DBObject) obj;
        Set<String> outKeySet = out.keySet();
        Assert.assertEquals(1, outKeySet.size());
        
        DBObject mOut = (DBObject) out.get("m");
        outKeySet = mOut.keySet();
        Assert.assertEquals(3, outKeySet.size());
        
        DBObject nmOut = (DBObject) mOut.get("nm");
        Assert.assertEquals(1, nmOut.get("a"));
        Assert.assertEquals(2, nmOut.get("b"));
        
        DBObject ntOut = (DBObject) mOut.get("nt");
        Assert.assertEquals(1, ntOut.get("0"));
        Assert.assertEquals(2, ntOut.get("1"));
        
        BasicDBList nbOut = (BasicDBList) mOut.get("nb");
        for (int i = 0; i < 3; i++) {
            DBObject nbtOut = (DBObject) nbOut.get(i);
            Assert.assertEquals(i, nbtOut.get("0"));
        }
    }
    
    @Test
    public void complexTuple() throws Exception {
        MongoStorage ms = new MongoStorage();
        BasicDBObject obj = new BasicDBObject();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(
            "t: (i: int, nt: (j: int, k: int))")
        );

        TupleFactory tupleFactory = TupleFactory.getInstance();

        Tuple t = tupleFactory.newTuple(2);
        t.set(0, new Integer(1));
        
        Tuple nt = tupleFactory.newTuple(2);
        nt.set(0, new Integer(2));
        nt.set(1, new Integer(3));
        t.set(1, nt);

        ms.writeField(obj, schema.getFields()[0], t);

        DBObject out = (DBObject) obj;
        Set<String> outKeySet = out.keySet();
        Assert.assertEquals(1, outKeySet.size());
        
        DBObject tOut = (DBObject) out.get("t");
        outKeySet = tOut.keySet();
        Assert.assertEquals(2, outKeySet.size());
        
        Assert.assertEquals(1, tOut.get("i"));
        DBObject ntOut = (DBObject) tOut.get("nt");
        Assert.assertEquals(2, ntOut.get("j"));
        Assert.assertEquals(3, ntOut.get("k"));
    }
    
    @Test
    public void complexBag() throws Exception {
        MongoStorage ms = new MongoStorage();
        BasicDBObject obj = new BasicDBObject();
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(
            "b: {t: (i: int, nb: {nt: (j: int)})}")
        );
        
        BagFactory bagFactory = BagFactory.getInstance();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        
        DataBag bag = bagFactory.newDefaultBag();
        for (int i = 0; i < 3; i++) {
            Tuple t = tupleFactory.newTuple(2);
            t.set(0, new Integer(i*3));
            
            DataBag nb = bagFactory.newDefaultBag();
            for (int j = 1; j < 4; j++) {
                Tuple nt = tupleFactory.newTuple(1);
                nt.set(0, new Integer(i*3 + j));
                nb.add(nt);
            }
            t.set(1, nb);
            
            bag.add(t);
        }
        
        ms.writeField(obj, schema.getFields()[0], bag);

        DBObject out = (DBObject) obj;
        Set<String> outKeySet = out.keySet();
        Assert.assertEquals(1, outKeySet.size());
        
        BasicDBList bagOut = (BasicDBList) out.get("b");
        for (int i = 0; i < 3; i++) {
            DBObject tOut = (DBObject) bagOut.get(i);
            outKeySet = tOut.keySet();
            Assert.assertEquals(2, outKeySet.size());
            
            Assert.assertEquals(i*3, tOut.get("i"));
            
            BasicDBList nbOut = (BasicDBList) tOut.get("nb");
            for (int j = 1; j < 4; j++) {
                DBObject ntOut = (DBObject) nbOut.get(j-1);
                Assert.assertEquals(i*3 + j, ntOut.get("j"));
            }
        }
    }
}
