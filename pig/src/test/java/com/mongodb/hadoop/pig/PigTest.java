package com.mongodb.hadoop.pig;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;


public class PigTest extends BaseHadoopTest {
    private static final Logger LOG = LoggerFactory.getLogger(PigTest.class);

    private static final MongoClientURI URI =
      new MongoClientURI("mongodb://localhost:27017/mongo_hadoop.pigtests");
    private MongoClient mongoClient;

    @Before
    public void setup() throws UnknownHostException {
        mongoClient = new MongoClient(URI);
        mongoClient.getDB("mongo_hadoop").getCollection("update_mus").drop();
    }

    @After
    public void tearDown() {
        mongoClient.close();
    }

    public void runMongoUpdateStorageTest(
      final String scriptName, final String[] expected)
      throws IOException, ParseException {
        org.apache.pig.pigunit.PigTest pigTest = new org.apache.pig.pigunit
          .PigTest(getClass().getResource(scriptName).getPath());

        // Let the STORE statement do its job so we can test MongoUpdateStorage.
        pigTest.unoverride("STORE");

        pigTest.assertOutput("results", expected);
    }

    @Test
    public void mongoUpdateStorage() throws IOException, ParseException {
        runMongoUpdateStorageTest(
          "/pig/update_simple_mus.pig",
          new String[] {
            "(Daniel,Alabi,([car#a],[car#b],[car#c],[car#a],[car#b],[car#c]))",
            "(Tolu,Alabi,([car#d],[car#e],[car#f],[car#d],[car#e],[car#f]))",
            "(Tinuke,Dada,([car#g],[car#g]))"
          }
        );
    }

    @Test
    public void mongoUpdateStorageMulti() throws IOException, ParseException {
        runMongoUpdateStorageTest(
          "/pig/update_age_alabis_mus.pig",
          new String[] {
            "(Daniel,Alabi,22.0)",
            "(Tolu,Alabi,24.0)",
            "(Tinuke,Dada,53.0)"
          }
        );
    }

}
