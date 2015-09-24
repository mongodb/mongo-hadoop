package com.mongodb.hadoop.util;

import com.mongodb.MongoClientURI;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MongoConfigUtilTest {

    private void assertSameURIs(
      final String[] expected, final List<MongoClientURI> actual) {
        assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; ++i) {
            assertEquals(expected[i], actual.get(i).getURI());
        }
    }

    @Test
    public void testGetMongoURIs() {
        Configuration conf = new Configuration();
        String[] connStrings = new String[] {
          "mongodb://rshost1:10000,rshost2:10001/foo.bar?replicaSet=rs",
          "mongodb://standalone:27017/db.collection"
        };

        // Separated by ", "
        conf.set(
          MongoConfigUtil.INPUT_URI,
          connStrings[0] + ", " + connStrings[1]);
        List<MongoClientURI> uris = MongoConfigUtil.getMongoURIs(
          conf, MongoConfigUtil.INPUT_URI);
        assertSameURIs(connStrings, uris);

        // No delimiter
        conf.set(MongoConfigUtil.INPUT_URI, connStrings[0] + connStrings[1]);
        uris = MongoConfigUtil.getMongoURIs(conf, MongoConfigUtil.INPUT_URI);
        assertSameURIs(connStrings, uris);

        // No value set
        uris = MongoConfigUtil.getMongoURIs(conf, "this key does not exist");
        assertEquals(0, uris.size());

        // Only one input URI.
        String connString = connStrings[1];
        conf.set(MongoConfigUtil.INPUT_URI, connString);
        uris = MongoConfigUtil.getMongoURIs(conf, MongoConfigUtil.INPUT_URI);
        assertSameURIs(new String[] {connString}, uris);
    }

}
