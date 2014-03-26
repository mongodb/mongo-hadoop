package org.mongodb.hadoop;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BaseHadoopTest {
    private static final Log LOG = LogFactory.getLog(BaseHadoopTest.class);

    public static String HADOOP_HOME;
    public static String HADOOP_VERSION = loadProperty("HADOOP_VERSION", "2.3");

    public static final File JSONFILE_PATH = new File("../examples/treasury_yield/src/main/resources/yield_historical_in.json");
    protected static final File JOBJAR_PATH = new File("../examples/treasury_yield/build/libs").listFiles(new HadoopVersionFilter())[0];

    protected static final List<DBObject> reference = new ArrayList<DBObject>();

    private static final String MONGO_IMPORT;

    private boolean noAuth = true;
    private MongoClient client;

    static {
        String property = System.getProperty("mongodb_server");
        String serverType = property != null ? property.replaceAll("-release", "") : "UNKNOWN";
        String path = format("/mnt/jenkins/mongodb/%s/%s/bin/mongoimport", serverType, property);
        MONGO_IMPORT = new File(path).exists() ? path : "/usr/local/bin/mongoimport";

        try {
            HADOOP_HOME = new File(loadProperty("HADOOP_HOME", "../../hadoop-binaries/hadoop-2.3.0")).getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }

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
    }

    private static String loadProperty(final String name, final String defaultValue) {
        String property = System.getProperty(name, System.getenv(name.toUpperCase()));
        if (property == null) {
            property = defaultValue;
        }
        return property;
    }


    protected static DBObject dbObject(Object... values) {
        BasicDBObject object = new BasicDBObject();
        for (int i = 0; i < values.length; i += 2) {
            object.append(values[i].toString(), values[i + 1]);
        }
        return object;
    }

    public void mongoImport(final String collection, final File file) {
        ProcessResult result = null;
        try {
            result = new ProcessExecutor().command(MONGO_IMPORT,
                                                   "--db", "mongo_hadoop",
                                                   "--collection", collection,
                                                   "--file", file.getAbsolutePath())
                                          .readOutput(true)
                                          .redirectOutput(System.out)
                                          .execute();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Before
    public void setUp() {
        getClient().getDB("mongo_hadoop").dropDatabase();

        mongoImport("yield_historical.in", JSONFILE_PATH);
    }

    public MongoClient getClient() {
        if(client == null) {
            try {
                client = new MongoClient();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return client;
    }

    public boolean isNoAuth() {
        return noAuth;
    }

    public void setNoAuth(final boolean noAuth) {
        this.noAuth = noAuth;
    }

    List<DBObject> asList(final DBCursor cursor) {
        List<DBObject> list = new ArrayList<DBObject>();
        while (cursor.hasNext()) {
            list.add(cursor.next());
        }

        return list;
    }


    protected boolean isStandalone() {
        return !isReplicaSet() && !isSharded();
    }

    protected boolean isReplicaSet() {
        return runIsMaster().get("setName") != null;
    }

    protected boolean isSharded() {
        CommandResult isMasterResult = runIsMaster();
        Object msg = isMasterResult.get("msg");
        return msg != null && msg.equals("isdbgrid");
    }

    protected CommandResult runIsMaster() {
        // Check to see if this is a replica set... if not, get out of here.
        return client.getDB("admin").command(new BasicDBObject("ismaster", 1));
    }

    protected void compareResults(final DBCollection collection, List<DBObject> expected) {
        List<DBObject> output = asList(collection.find().sort(new BasicDBObject("_id", 1)));
        assertEquals("count is not same: " + output, expected.size(), output.size());
        for (int i = 0; i < output.size(); i++) {
            final DBObject doc = output.get(i);
            // round to account for slight changes due to precision in case ops are run in different order.
            DBObject referenceDoc = expected.get(i);
            if (!doc.get("_id").equals(referenceDoc.get("_id"))
                || !doc.get("count").equals(referenceDoc.get("count"))
                || !round((Double) doc.get("avg"), 7).equals(round((Double) referenceDoc.get("avg"), 7))) {

                fail(format("docs do not match: %s%n vs %s", doc, referenceDoc));
            }
        }
    }

    private BigDecimal round(final Double value, final int precision) {
        return new BigDecimal(value).round(new MathContext(precision));
    }

    public void runJob(Map<String, String> params, String className, String[] inputCollections, String[] outputUris) {
        try {
            copyJars();
            List<String> cmd = new ArrayList<String>();
            cmd.add(new File(HADOOP_HOME, "bin/hadoop").getCanonicalPath());
            cmd.add("jar");
            cmd.add(JOBJAR_PATH.getAbsolutePath());
            cmd.add(className);

            for (Entry<String, String> entry : params.entrySet()) {
                cmd.add(format("-D%s=%s", entry.getKey(), entry.getValue()));
            }
            if (inputCollections != null && inputCollections.length != 0) {
                StringBuilder inputUri = new StringBuilder();
                for (String inputCollection : inputCollections) {
                    inputUri.append(format("mongodb://localhost/%s ", inputCollection));
                }
                cmd.add(format("-Dmongo.input.uri=%s", inputUri.toString().trim()));
            }
            if (outputUris != null && outputUris.length != 0) {
                StringBuilder outputUri = new StringBuilder();
                for (String uri : outputUris) {
                    outputUri.append(format("%s ", uri));
                }
                cmd.add(format("-Dmongo.output.uri=%s", outputUri.toString().trim()));
            }
            
            LOG.info("Executing hadoop job:");

            StringBuilder output = new StringBuilder();
            Iterator<String> iterator = cmd.iterator();
            while (iterator.hasNext()) {
                final String s = iterator.next();
                if (output.length() != 0) {
                    output.append("\t");
                } else {
                    output.append("\n");
                }
                output.append(s);
                if (iterator.hasNext()) {
                    output.append(" \\");
                }
                output.append("\n");
            }

            LOG.info(output);
            new ProcessExecutor().command(cmd)
                                 .redirectError(System.out)
                                 .execute();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void copyJars() {
        String hadoopLib;
        hadoopLib = format(HADOOP_VERSION.startsWith("1") ? "../../hadoop-binaries/hadoop-${hadoopVersion}/lib"
                                                   : "../../hadoop-binaries/hadoop-${hadoopVersion}/share/hadoop/common");
        try {
            URLClassLoader classLoader = (URLClassLoader) getClass().getClassLoader();
            for (URL url : classLoader.getURLs()) {
                boolean contains = url.getPath().contains("mongo-java-driver");
                if (contains) {
                    File file = new File(url.toURI());
                    FileUtils.copyFile(file, new File(hadoopLib, "mongo-java-driver.jar"));
                }
            }
            File coreJar = new File(format("../core/build/libs")).listFiles(new HadoopVersionFilter())[0];
            FileUtils.copyFile(coreJar, new File(hadoopLib, "mongo-hadoop-core.jar"));
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static class HadoopVersionFilter implements FileFilter {
        @Override
        public boolean accept(final File pathname) {
            return pathname.getName().endsWith(format("_%s.jar", HADOOP_VERSION));
        }
    }
}
