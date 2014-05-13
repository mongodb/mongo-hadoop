package org.mongodb.hadoop;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.splitter.MultiMongoCollectionSplitter;
import com.mongodb.hadoop.splitter.SingleMongoSplitter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.hadoop.splitter.MultiMongoCollectionSplitter.MULTI_COLLECTION_CONF_KEY;
import static com.mongodb.hadoop.util.MongoConfigUtil.INPUT_NOTIMEOUT;
import static com.mongodb.hadoop.util.MongoConfigUtil.INPUT_URI;
import static com.mongodb.hadoop.util.MongoConfigUtil.MONGO_SPLITTER_CLASS;
import static com.mongodb.hadoop.util.MongoConfigUtil.SPLITS_USE_RANGEQUERY;
import static org.junit.Assume.assumeFalse;

public class TestStandalone extends BaseHadoopTest {
    private static final Log LOG = LogFactory.getLog(TestStandalone.class);
    private final MongoClientURI inputUri2;

    public TestStandalone() {
        inputUri2 = authCheck(new MongoClientURIBuilder()
                                  .collection("mongo_hadoop", "yield_historical.in2"))
                        .build();
    }

    @Before
    public void checkConfiguration() {
        assumeFalse(isSharded());
    }

    @Test
    public void testBasicInputSource() {
        LOG.info("testing basic input source");
        new MapReduceJob("com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig")
            .param("mongo.input.notimeout", "true")
            .inputUris(getInputUri())
            .outputUris(getOutputUri())
            .execute(isRunTestInVm());
        compareResults(getClient().getDB(getOutputUri().getDatabase()).getCollection(getOutputUri().getCollection()), getReference());
    }


    @Test
    public void testTreasuryJsonConfig() {
        mongoImport("yield_historical.in3", TREASURY_JSON_PATH);
        new MapReduceJob("com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig")
            .param(MONGO_SPLITTER_CLASS, MultiMongoCollectionSplitter.class.getName())
            .param(MULTI_COLLECTION_CONF_KEY, collectionSettings().toString())
            .outputUris(getOutputUri())
            .execute(isRunTestInVm());

        compareDoubled(getClient().getDB(getOutputUri().getDatabase()).getCollection(getOutputUri().getCollection()));
    }

    @Test
    public void testMultipleCollectionSupport() {
        mongoImport(getInputUri().getCollection(), TREASURY_JSON_PATH);
        mongoImport(inputUri2.getCollection(), TREASURY_JSON_PATH);
        new MapReduceJob("com.mongodb.hadoop.examples.treasury.TreasuryYieldXMLConfig")
            .param(MONGO_SPLITTER_CLASS, MultiMongoCollectionSplitter.class.getName())
            .inputUris(getInputUri(), inputUri2)
            .outputUris(getOutputUri())
            .execute(isRunTestInVm());

        compareDoubled(getClient().getDB(getOutputUri().getDatabase()).getCollection(getOutputUri().getCollection()));
    }

    private void compareDoubled(final DBCollection out) {
        List<DBObject> referenceDoubled = new ArrayList<DBObject>();
        for (DBObject object : getReference()) {
            DBObject doubled = new BasicDBObject();
            doubled.putAll(object);
            referenceDoubled.add(doubled);
            Integer count = ((Integer) object.get("count")) * 2;
            Double sum = ((Double) object.get("sum")) * 2;

            doubled.put("count", count);
            doubled.put("avg", sum / count);
            doubled.put("sum", sum);
        }

        compareResults(out, referenceDoubled);
    }

    private JsonNode collectionSettings() {
        ArrayNode settings = new ArrayNode(JsonNodeFactory.instance);
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put(INPUT_URI, getInputUri().toString());
        ObjectNode dow = new ObjectNode(JsonNodeFactory.instance);
        dow.put("dayOfWeek", "FRIDAY");
        node.put("query", dow);
        node.put(MONGO_SPLITTER_CLASS, SingleMongoSplitter.class.getName());
        node.put(SPLITS_USE_RANGEQUERY, true);
        node.put(INPUT_NOTIMEOUT, true);
        settings.add(node);

        MongoClientURI inputUri3 = authCheck(new MongoClientURIBuilder()
                                                 .collection("mongo_hadoop", "yield_historical.in3"))
                                       .build();

        node = new ObjectNode(JsonNodeFactory.instance);
        node.put(INPUT_URI, inputUri3.toString());
        node.put(SPLITS_USE_RANGEQUERY, true);
        node.put(INPUT_NOTIMEOUT, true);
        settings.add(node);

        return settings;
    }
}
