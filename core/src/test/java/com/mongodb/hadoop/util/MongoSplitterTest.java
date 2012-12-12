package com.mongodb.hadoop.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import com.mongodb.MongoURI;
import com.mongodb.hadoop.input.TestMongoInputSplit;

public class MongoSplitterTest {

    @Test
    public void testCreateSplitList_oneShard() {
        int numChunks = 2;
        InputSplit split1 = new TestMongoInputSplit(new MongoURI("mongodb://split1"));
        InputSplit split2 = new TestMongoInputSplit(new MongoURI("mongodb://split2"));
        LinkedList<InputSplit> shardSplits = new LinkedList<InputSplit>(Arrays.asList(split1, split2));
        
        Map<String,LinkedList<InputSplit>> shardToSplits = new HashMap<String, LinkedList<InputSplit>>();
        shardToSplits.put("shard1", shardSplits);
        
        List<InputSplit> splits = MongoSplitter.createSplitList(numChunks, shardToSplits);
        assertEquals(split1, splits.get(0));
        assertEquals(split2, splits.get(1));
    }
    
    @Test
    public void testCreateSplitList_twoEvenShards() {
        int numChunks = 4;
        InputSplit split1 = new TestMongoInputSplit(new MongoURI("mongodb://split1"));
        InputSplit split2 = new TestMongoInputSplit(new MongoURI("mongodb://split2"));
        InputSplit split3 = new TestMongoInputSplit(new MongoURI("mongodb://split3"));
        InputSplit split4 = new TestMongoInputSplit(new MongoURI("mongodb://split4"));
        LinkedList<InputSplit> shardSplits1 = new LinkedList<InputSplit>(Arrays.asList(split1, split2));
        LinkedList<InputSplit> shardSplits2 = new LinkedList<InputSplit>(Arrays.asList(split3, split4));
        
        Map<String,LinkedList<InputSplit>> shardToSplits = new HashMap<String, LinkedList<InputSplit>>();
        shardToSplits.put("shard1", shardSplits1);
        shardToSplits.put("shard2", shardSplits2);
        
        List<InputSplit> splits = MongoSplitter.createSplitList(numChunks, shardToSplits);
        assertEquals(split1, splits.get(0));
        assertEquals(split3, splits.get(1));
        assertEquals(split2, splits.get(2));
        assertEquals(split4, splits.get(3));
    }
    
    @Test
    public void testCreateSplitList_twoUnevenShards() {
        int numChunks = 6;
        InputSplit split1 = new TestMongoInputSplit(new MongoURI("mongodb://split1"));
        InputSplit split2 = new TestMongoInputSplit(new MongoURI("mongodb://split2"));
        InputSplit split3 = new TestMongoInputSplit(new MongoURI("mongodb://split3"));
        InputSplit split4 = new TestMongoInputSplit(new MongoURI("mongodb://split4"));
        InputSplit split5 = new TestMongoInputSplit(new MongoURI("mongodb://split5"));
        InputSplit split6 = new TestMongoInputSplit(new MongoURI("mongodb://split6"));
        LinkedList<InputSplit> shardSplits1 = new LinkedList<InputSplit>(Arrays.asList(split1, split2));
        LinkedList<InputSplit> shardSplits2 = new LinkedList<InputSplit>(Arrays.asList(split3, split4, split5, split6));
        
        Map<String,LinkedList<InputSplit>> shardToSplits = new HashMap<String, LinkedList<InputSplit>>();
        shardToSplits.put("shard1", shardSplits1);
        shardToSplits.put("shard2", shardSplits2);
        
        List<InputSplit> splits = MongoSplitter.createSplitList(numChunks, shardToSplits);
        assertEquals(split1, splits.get(0));
        assertEquals(split3, splits.get(1));
        assertEquals(split2, splits.get(2));
        assertEquals(split4, splits.get(3));
        assertEquals(split5, splits.get(4));
        assertEquals(split6, splits.get(5));
    }
}
