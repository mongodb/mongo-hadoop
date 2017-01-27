package com.mongodb.hadoop.io;

import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class MongoInputSplitTest {

    @Test
    public void testConstructor() {
        Configuration conf = new Configuration();
        MongoConfigUtil.setFields(conf, "{\"field\": 1}");
        MongoConfigUtil.setAuthURI(conf, "mongodb://auth");
        MongoConfigUtil.setInputURI(conf, "mongodb://input");
        MongoConfigUtil.setInputKey(conf, "field");
        MongoConfigUtil.setMaxSplitKey(conf, "{\"field\": 1e9}");
        MongoConfigUtil.setMinSplitKey(conf, "{\"field\": -1e9}");
        MongoConfigUtil.setNoTimeout(conf, true);
        MongoConfigUtil.setQuery(conf, "{\"foo\": 42}");
        MongoConfigUtil.setSort(conf, "{\"foo\": -1}");
        MongoConfigUtil.setSkip(conf, 10);

        MongoInputSplit mis = new MongoInputSplit(conf);

        assertEquals(MongoConfigUtil.getFields(conf), mis.getFields());
        assertEquals(MongoConfigUtil.getAuthURI(conf), mis.getAuthURI());
        assertEquals(MongoConfigUtil.getInputURI(conf), mis.getInputURI());
        assertEquals(MongoConfigUtil.getInputKey(conf), mis.getKeyField());
        assertEquals(MongoConfigUtil.getMaxSplitKey(conf), mis.getMax());
        assertEquals(MongoConfigUtil.getMinSplitKey(conf), mis.getMin());
        assertEquals(MongoConfigUtil.isNoTimeout(conf), mis.getNoTimeout());
        assertEquals(MongoConfigUtil.getQuery(conf), mis.getQuery());
        assertEquals(MongoConfigUtil.getSort(conf), mis.getSort());
        assertEquals(MongoConfigUtil.getLimit(conf), (int) mis.getLimit());
        assertEquals(MongoConfigUtil.getSkip(conf), (int) mis.getSkip());

        MongoInputSplit mis2 = new MongoInputSplit(mis);
        assertEquals(mis, mis2);
    }
}
