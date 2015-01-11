package com.mongodb.hadoop;

import com.jayway.awaitility.Awaitility;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class TreasuryTest extends BaseHadoopTest {
    private static final Logger LOG = LoggerFactory.getLogger(TreasuryTest.class);
    
    public static final File TREASURY_YIELD_HOME;
    public static final File TREASURY_JSON_PATH;
    private final MongoClientURI outputUri;
    private final MongoClientURI inputUri;
    private final List<DBObject> reference = new ArrayList<DBObject>();
    protected static final File JOBJAR_PATH;

    static {
        TREASURY_YIELD_HOME = new File(PROJECT_HOME, "examples/treasury_yield");
        TREASURY_JSON_PATH = new File(TreasuryTest.TREASURY_YIELD_HOME, "/src/main/resources/yield_historical_in.json");
        JOBJAR_PATH = findProjectJar(TreasuryTest.TREASURY_YIELD_HOME);
    }

    public TreasuryTest() {
        reference.add(dbObject("_id", 1990, "count", 250, "avg", 8.552400000000002, "sum", 2138.1000000000004));
        reference.add(dbObject("_id", 1991, "count", 250, "avg", 7.8623600000000025, "sum", 1965.5900000000006));
        reference.add(dbObject("_id", 1992, "count", 251, "avg", 7.008844621513946, "sum", 1759.2200000000005));
        reference.add(dbObject("_id", 1993, "count", 250, "avg", 5.866279999999999, "sum", 1466.5699999999997));
        reference.add(dbObject("_id", 1994, "count", 249, "avg", 7.085180722891565, "sum", 1764.2099999999996));
        reference.add(dbObject("_id", 1995, "count", 250, "avg", 6.573920000000002, "sum", 1643.4800000000005));
        reference.add(dbObject("_id", 1996, "count", 252, "avg", 6.443531746031742, "sum", 1623.769999999999));
        reference.add(dbObject("_id", 1997, "count", 250, "avg", 6.353959999999992, "sum", 1588.489999999998));
        reference.add(dbObject("_id", 1998, "count", 250, "avg", 5.262879999999994, "sum", 1315.7199999999984));
        reference.add(dbObject("_id", 1999, "count", 251, "avg", 5.646135458167332, "sum", 1417.1800000000003));
        reference.add(dbObject("_id", 2000, "count", 251, "avg", 6.030278884462145, "sum", 1513.5999999999985));
        reference.add(dbObject("_id", 2001, "count", 248, "avg", 5.02068548387097, "sum", 1245.1300000000006));
        reference.add(dbObject("_id", 2002, "count", 250, "avg", 4.61308, "sum", 1153.27));
        reference.add(dbObject("_id", 2003, "count", 250, "avg", 4.013879999999999, "sum", 1003.4699999999997));
        reference.add(dbObject("_id", 2004, "count", 250, "avg", 4.271320000000004, "sum", 1067.8300000000008));
        reference.add(dbObject("_id", 2005, "count", 250, "avg", 4.288880000000001, "sum", 1072.2200000000003));
        reference.add(dbObject("_id", 2006, "count", 250, "avg", 4.7949999999999955, "sum", 1198.7499999999989));
        reference.add(dbObject("_id", 2007, "count", 251, "avg", 4.634661354581674, "sum", 1163.3000000000002));
        reference.add(dbObject("_id", 2008, "count", 251, "avg", 3.6642629482071714, "sum", 919.73));
        reference.add(dbObject("_id", 2009, "count", 250, "avg", 3.2641200000000037, "sum", 816.0300000000009));
        reference.add(dbObject("_id", 2010, "count", 189, "avg", 3.3255026455026435, "sum", 628.5199999999996));
        inputUri = authCheck(new MongoClientURIBuilder()
                                 .collection("mongo_hadoop", "yield_historical.in")).build();
        outputUri = authCheck(new MongoClientURIBuilder()
                                  .collection("mongo_hadoop", "yield_historical.out")).build();
    }

    @Before
    public void setUp() {
        dropMongoHadoop();
        mongoImport("yield_historical.in", TreasuryTest.TREASURY_JSON_PATH);
    }

    @After
    public void dropMongoHadoop() {
        try {
            Awaitility
                .await()
                .atMost(5, TimeUnit.MINUTES)
                .until(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        try {
                            getClient(getInputUri()).getDB("mongo_hadoop").dropDatabase();
                            return true;
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                            return false;
                        }
                    }
                });
        } catch (Exception e) {
            LOG.info(e.getMessage(), e);
        }
    }

    protected void compareDoubled(final DBCollection out) {
        List<DBObject> referenceDoubled = new ArrayList<DBObject>();
        for (DBObject object : getReference()) {
            DBObject doubled = new BasicDBObject();
            doubled.putAll(object);
            referenceDoubled.add(doubled);
            Integer count = (Integer) object.get("count") * 2;
            Double sum = (Double) object.get("sum") * 2;

            doubled.put("count", count);
            doubled.put("avg", sum / count);
            doubled.put("sum", sum);
        }

        compareResults(out, referenceDoubled);
    }

    protected void compareResults(final DBCollection collection, final List<DBObject> expected) {
        List<DBObject> output = toList(collection.find().sort(new BasicDBObject("_id", 1)));
        assertEquals("count is not same: " + output, expected.size(), output.size());
        for (int i = 0; i < output.size(); i++) {
            final DBObject doc = output.get(i);
            // round to account for slight changes due to precision in case ops are run in different order.
            DBObject referenceDoc = expected.get(i);
            assertEquals(format("IDs[%s] do not match: %s%n vs %s", i, doc, referenceDoc), doc.get("_id"), referenceDoc.get("_id"));
            assertEquals(format("counts[%s] do not match: %s%n vs %s", i, doc, referenceDoc), doc.get("count"), referenceDoc.get("count"));
            assertEquals(format("averages[%s] do not match: %s%n vs %s", i, doc, referenceDoc),
                         round((Double) doc.get("avg"), 7),
                         round((Double) referenceDoc.get("avg"), 7));
        }
    }

    private BigDecimal round(final Double value, final int precision) {
        return new BigDecimal(value).round(new MathContext(precision));
    }

    public List<DBObject> getReference() {
        return reference;
    }

    public MongoClientURI getOutputUri() {
        return outputUri;
    }

    public MongoClientURI getInputUri() {
        return inputUri;
    }
}
