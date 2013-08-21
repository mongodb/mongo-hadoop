package com.mongodb.hadoop.pig;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.bson.BasicBSONObject;
import java.util.ArrayList;
import java.util.Map;

/*
 * Unit tests for JSONPigReplace
 */
public class JSONPigReplaceTest {
    private static TupleFactory tupleFactory;
    private static BagFactory bagFactory;
    
    @Before
    public void setUp() throws Exception {
        tupleFactory = TupleFactory.getInstance();
        bagFactory = BagFactory.getInstance();
    }
    
    @Test
    public void testNonNestedReplace() throws Exception {
        // create tuple ("Daniel", "Alabi")
        // with schema 'first:chararray, last:chararray'
        Tuple t = tupleFactory.newTuple(2);
        t.set(0, "Daniel");
        t.set(1,  "Alabi");
        
        JSONPigReplace j = new JSONPigReplace(
                                              new String[] {
                                                  "{first: '$first', last: '$last'}"
                                              }
                                              );
        BasicBSONObject[] bs = j.substitute(t, "first:chararray, last:chararray", null);
        
        assertNotNull(bs);
        assertTrue(bs.length == 1);
        
        // should produce BSONObject 
        // {first:"Daniel", last:"Alabi"}
        BasicBSONObject res = bs[0];
        assertEquals(res.get("first"), "Daniel");
        assertEquals(res.get("last"), "Alabi");
    } 
    
    @Test
    public void testNamedArrayReplace() throws Exception {
        // create tuple ({("a"), ("b"), ("c")}) 
        // with schema 'cars:{f:(t:chararray)}'
        DataBag b = bagFactory.newDefaultBag();
        b.add(tupleFactory.newTuple("a"));
        b.add(tupleFactory.newTuple("b"));
        b.add(tupleFactory.newTuple("c"));
        
        JSONPigReplace j = new JSONPigReplace(
                                              new String[] {
                                                  "{days : [1,2,3], age : 19, cars : '$cars'}"
                                              }
                                              );
        BasicBSONObject[] bs = j.substitute(tupleFactory.newTuple(b), 
                                            "cars : {f:(t:chararray)}", null);
        
        assertNotNull(bs);
        assertTrue(bs.length == 1);
        
        // should produce BSONObject
        // { "days" : [ 1 , 2 , 3] , "age" : 19 , "cars" : [ { "t" : "a"} , { "t" : "b"} , { "t" : "c"}]}
        BasicBSONObject res = bs[0];
        ArrayList cars = (ArrayList) res.get("cars");
        assertEquals(cars.size(), 3);
        Object o = cars.get(0);
        assertEquals(((Map)o).get("t"), "a");
    }
    
    @Test 
    public void testUnnamedArrayReplace() throws Exception {
        // create tuple ({("a"), ("b"), ("c")}) 
        // with schema 'cars:{f:(t:chararray)}'
        DataBag b = bagFactory.newDefaultBag();
        b.add(tupleFactory.newTuple("a"));
        b.add(tupleFactory.newTuple("b"));
        b.add(tupleFactory.newTuple("c"));
        
        JSONPigReplace j = new JSONPigReplace(
                                              new String[] {
                                                  "{days : [1,2,3], age : 19, cars : '$cars'}"
                                              }
                                              );
        BasicBSONObject[] bs = j.substitute(tupleFactory.newTuple(b), 
                                            "cars : {f:(t:chararray)}",
                                            "t");
        assertNotNull(bs);
        assertTrue(bs.length == 1); 
        
        // should produce
        // { "name" : "Daniel" , "age" : 19 , "property" : { "cars" : [ "a" , "b" , "c"]} , "school" : "Carleton College"}
        BasicBSONObject res = bs[0];
        ArrayList cars = (ArrayList) res.get("cars");
        assertEquals(cars.size(), 3);
        assertEquals(cars.get(0), "a");
    }
    
    @Test
    public void testSimpleNestedReplace() throws Exception {
        // create tuple ({("Daniel", "Alabi")}, "Carleton College")
        // with schema 'b:{t:(f:chararray,l:chararray)}, s:chararray'
        Tuple t1 = tupleFactory.newTuple(2);
        t1.set(0, "Daniel");
        t1.set(1, "Alabi");
        DataBag b = bagFactory.newDefaultBag();
        b.add(t1);
        Tuple t = tupleFactory.newTuple(2);
        t.set(0, b);
        t.set(1, "Carleton College");
        
        JSONPigReplace j = new JSONPigReplace(
                                              new String[] {
                                                  "{first:'$f', last:'$l', school:'$s'}"
                                              }
                                              );
        BasicBSONObject[] bs = j.substitute(t, 
                                            "b:{t:(f:chararray,l:chararray)}, s:chararray",
                                            null);
        assertNotNull(bs);
        assertTrue(bs.length == 1); 
        
        // should produce
        // { "first" : "Daniel" , "last" : "Alabi" , "school" : "Carleton College"}
        BasicBSONObject res = bs[0];
        assertEquals(res.get("first"), "Daniel");
        assertEquals(res.get("last"), "Alabi");
        assertEquals(res.get("school"), "Carleton College");
    }
    
    @Test
    public void testSimpleMultipleReplace() throws Exception {
        // create tuple ({("Daniel", "Alabi")}, "Carleton College")
        // with schema 'b:{b:(f:chararray,l:chararray)}, s:chararray'
        Tuple t1 = tupleFactory.newTuple(2);
        t1.set(0, "Daniel");
        t1.set(1, "Alabi");
        DataBag b = bagFactory.newDefaultBag();
        b.add(t1);
        Tuple t = tupleFactory.newTuple(2);
        t.set(0, b);
        t.set(1, "Carleton College");
        
        JSONPigReplace j = new JSONPigReplace(
                                              new String[] {
                                                  "{first:'$f', last:'$l', school:'$s'}",
                                                  "{$push : {schools: '$s'}}"
                                              }
                                              );
    BasicBSONObject[] bs = j.substitute(t, 
                                        "b:{t:(f:chararray,l:chararray)}, s:chararray",
                                        null);
    assertNotNull(bs);
    assertTrue(bs.length == 2); 
    
    // should produce
    // { "first" : "Daniel" , "last" : "Alabi" , "school" : "Carleton College"}
    // and
    // { "$push" : { "schools" : "Carleton College"}}
    BasicBSONObject res1 = bs[0];
    BasicBSONObject res2 = bs[1];
    assertEquals(res1.get("first"), "Daniel");
    assertEquals(res1.get("last"), "Alabi");
    assertEquals(((BasicBSONObject)res2.get("$push")).get("schools"),
                 "Carleton College");                   
    }
    
    @Test
    public void testSampleQueryUpdateReplace() throws Exception {
        // create tuple ("Daniel", "Alabi", 19, {("a"), ("b"), ("c")})
        // with schema 'f:chararray,l:chararray,age:int,cars:{t:(t:chararray)}'
        DataBag b = bagFactory.newDefaultBag();
        b.add(tupleFactory.newTuple("a"));
        b.add(tupleFactory.newTuple("b"));
        b.add(tupleFactory.newTuple("c"));
        Tuple t = tupleFactory.newTuple(4);
        t.set(0, "Daniel");
        t.set(1, "Alabi");
        t.set(2, 19);
        t.set(3, b);
        JSONPigReplace j = new JSONPigReplace(
                                              new String[] {"{first:'$f', last:'$l'}",
                                                            "{$set: {age: '$age'}, $pushAll : {cars: '$cars'}}"}
                                              );
        BasicBSONObject[] bs = j.substitute(t,
                                            "f:chararray,l:chararray,age:int,cars:{t:(t:chararray)}",
                                            "t");
        assertTrue(bs.length == 2);
        
        // should produce
        // { "first" : "Daniel" , "last" : "Alabi"}
        // { "$set" : { "age" : 19} , "$pushAll" : { "cars" : [ "a" , "b" , "c"]}}
        BasicBSONObject res1 = bs[0];
        BasicBSONObject res2 = bs[1];
        assertEquals(res1.get("first"), "Daniel");
        assertEquals(((BasicBSONObject)res2.get("$set")).get("age"), 19);
    }
}
