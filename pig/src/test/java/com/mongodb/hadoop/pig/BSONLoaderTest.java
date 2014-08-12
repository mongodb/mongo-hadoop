package com.mongodb.hadoop.pig;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.data.Tuple;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.easymock.EasyMock;
import org.junit.Test;

public class BSONLoaderTest {
    
    @Test
    public void testSimpleObject() throws Exception {
        
        String schema = "id:chararray,"
        		      + "name:chararray,"
        		      + "books:bag{tuple(title:chararray)}";
        /*
           This goal of this test is to pass an instance of BasicBSONObject,
           NOT it's child BasicDBObject, to the BSONLoader class. It's possible
           to do the following
           
               // Creates a BasicDBObject which we don't want
               BasicBSONObject topLevelObject = (BasicBSONObject) JSON.parse(jsonDataStr);
               
           yet this creates a BasicDBObject under the hood. This is why the
           following BSONobject is constructed long hand below.
           
           Test BSON object:
                {"_id" : ObjectId("53ea61edc2e62b61021c2f2e"),
                 "name" : "Jane",
                 "books": [{"title":"Lord of the Rings"}]
                }
        */
        BasicBSONObject book1 = new BasicBSONObject();
        book1.append("title", "Lord of the Rings");
        BasicBSONList bookList = new BasicBSONList();
        bookList.add(book1);
        BasicBSONObject simpleObject = new BasicBSONObject();
        simpleObject.append("_id", new ObjectId(DatatypeConverter.parseHexBinary("53ea61edc2e62b61021c2f2e")));
        simpleObject.append("name", "Jane");
        simpleObject.append("books", bookList);
 
        RecordReader recordReader = EasyMock.createNiceMock(RecordReader.class);
        expect(recordReader.nextKeyValue()).andReturn(true).once();
        expect(recordReader.nextKeyValue()).andReturn(false);
        expect(recordReader.getCurrentValue()).andReturn(simpleObject).once();
        expect(recordReader.getCurrentValue()).andReturn(null);
        replay(recordReader);
     
        BSONLoader bsonLoader = new BSONLoader("id", schema);
        bsonLoader.prepareToRead(recordReader, null);
        Tuple t = bsonLoader.getNext();
        
        String expected = "(53ea61edc2e62b61021c2f2e,Jane,{(Lord of the Rings)})";
        assertEquals(expected, t.toString());
    }
    
}