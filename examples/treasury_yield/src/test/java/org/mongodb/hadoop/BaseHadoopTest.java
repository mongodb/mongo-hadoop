package org.mongodb.hadoop;

import com.jayway.awaitility.Awaitility;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class BaseHadoopTest {
    private static final Logger LOG = LoggerFactory.getLogger(BaseHadoopTest.class);

    public static final String HADOOP_HOME;
    public static final String HADOOP_VERSION = loadProperty("hadoop.version", "2.4");
    public static final String HADOOP_RELEASE_VERSION = loadProperty("hadoop.release.version", "2.4.0");

    public static final File PROJECT_HOME;
    public static final File TREASURY_YIELD_HOME;
    public static final File TREASURY_JSON_PATH;

    private final MongoClientURI outputUri;
    private final MongoClientURI inputUri;

    private static final boolean runTestInVm = Boolean.valueOf(System.getProperty("mongo.hadoop.testInVM", "false"));

    private final List<DBObject> reference = new ArrayList<DBObject>();

    private static final String MONGO_IMPORT;

    private MongoClient client;

    static {
        try {
            String property = System.getProperty("mongodb_server");
            String serverType = property != null ? property.replaceAll("-release", "") : "UNKNOWN";
            String path = format("/mnt/jenkins/mongodb/%s/%s/bin/mongoimport", serverType, property);
            MONGO_IMPORT = new File(path).exists() ? path : "/usr/local/bin/mongoimport";
            if (!new File(MONGO_IMPORT).exists()) {
                throw new RuntimeException(format("Can not locate mongoimport.  Tried looking in '%s' and '%s' assuming a server "
                                                  + "type of '%s'", path, "/usr/local/bin/mongoimport", property));
            }

            HADOOP_HOME = new File(String.format("%s/hadoop-binaries/hadoop-%s", System.getProperty("user.home"),
                                                 HADOOP_RELEASE_VERSION)).getCanonicalPath();

            File current = new File(".").getCanonicalFile();
            File home = new File(current, "examples/treasury_yield");
            while (!home.exists() && current.getParentFile().exists()) {
                current = current.getParentFile();
                home = new File(current, "examples/treasury_yield");
            }
            PROJECT_HOME = current;
            TREASURY_YIELD_HOME = home;
            TREASURY_JSON_PATH = new File(TREASURY_YIELD_HOME, "/src/main/resources/yield_historical_in.json");
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static MiniYARNCluster yarnCluster;
    private static MiniDFSCluster dfsCluster;

    public BaseHadoopTest() {
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

    protected MongoClientURIBuilder authCheck(final MongoClientURIBuilder builder) {
        if (isAuthEnabled()) {
            builder.auth("bob", "pwd123");
        }

        return builder;
    }

    protected static String loadProperty(final String name, final String defaultValue) {
        String property = System.getProperty(name, System.getenv(name.toUpperCase()));
        if (property == null) {
            property = defaultValue;
        }
        return property;
    }


    protected static DBObject dbObject(final Object... values) {
        BasicDBObject object = new BasicDBObject();
        for (int i = 0; i < values.length; i += 2) {
            object.append(values[i].toString(), values[i + 1]);
        }
        return object;
    }

    public void mongoImport(final String collection, final File file) {
        try {
            List<String> command = new ArrayList<String>();
            command.addAll(asList(MONGO_IMPORT,
                                  "--drop",
                                  "--db", "mongo_hadoop",
                                  "--collection", collection,
                                  "--file", file.getAbsolutePath()));
            if (isAuthEnabled()) {
                List<String> list = new ArrayList<String>(asList("-u", "bob",
                                                                 "-p", "pwd123"));
                if (!System.getProperty("mongodb_server", "").equals("22-release")) {
                    list.addAll(asList("--authenticationDatabase", "admin"));
                }
                command.addAll(list);
            }
            StringBuilder output = new StringBuilder();
            Iterator<String> iterator = command.iterator();
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
            LOG.info(output.toString());

            ProcessExecutor executor = new ProcessExecutor().command(command)
                                                            .readOutput(true)
                                                            .redirectOutput(System.out);
            ProcessResult result = executor.execute();
            if (result.getExitValue() != 0) {
                LOG.error(result.getOutput().getString());
                throw new RuntimeException("mongoimport failed.");
            }

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
        mongoImport("yield_historical.in", TREASURY_JSON_PATH);
    }

    public MongoClient getClient() {
        if (client == null) {
            try {
                MongoClientURI uri = getInputUri();
                client = new MongoClient(uri);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return client;
    }

    public List<DBObject> getReference() {
        return reference;
    }

    List<DBObject> toList(final DBCursor cursor) {
        List<DBObject> list = new ArrayList<DBObject>();
        while (cursor.hasNext()) {
            list.add(cursor.next());
        }

        return list;
    }

    protected static boolean isAuthEnabled() {
        return Boolean.valueOf(System.getProperty("authEnabled", "false"))
               || "auth".equals(System.getProperty("mongodb_option"));
    }

    protected boolean isSharded() {
        CommandResult isMasterResult = runIsMaster();
        Object msg = isMasterResult.get("msg");
        return msg != null && msg.equals("isdbgrid");
    }

    protected CommandResult runIsMaster() {
        // Check to see if this is a replica set... if not, get out of here.
        return getClient().getDB("admin").command(new BasicDBObject("ismaster", 1));
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

    @BeforeClass
    public static void setupCluster() {
        if (isRunTestInVm()) {
            try {
                System.setProperty("hadoop.log.dir", "/tmp/hadoop-test-logs");

                Configuration config = new Configuration();
                config.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
                config.setClass(YarnConfiguration.RM_SCHEDULER,
                                FifoScheduler.class, ResourceScheduler.class);
                config.set("yarn.log.dir", "/tmp/yarn-logs");
                dfsCluster = new MiniDFSCluster.Builder(config)
                                 .numDataNodes(1)
                                 .startupOption(StartupOption.FORMAT)
                                 .build();
                yarnCluster = new MiniYARNCluster("mongo-hadoop", 1, 1, 1, 1);
                yarnCluster.init(config);

                final ContainerManagerImpl cm = (ContainerManagerImpl) yarnCluster.getNodeManager(0).getNMContext().getContainerManager();
                Awaitility.await()
                          .atMost(2, MINUTES)
                          .until(new Callable<Boolean>() {
                              @Override
                              public Boolean call() throws Exception {
                                  return !cm.getBlockNewContainerRequestsStatus();
                              }
                          });

                URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
                if (url == null) {
                    throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
                }
                Configuration yarnClusterConfig = yarnCluster.getConfig();
                yarnClusterConfig.set("yarn.application.classpath", new File(url.getPath()).getParent());
                //write the document to a buffer (not directly to the file, as that
                //can cause the file being written to get read -which will then fail.
                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                yarnClusterConfig.writeXml(bytesOut);
                bytesOut.close();
                //write the bytes to the file in the classpath
                OutputStream os = new FileOutputStream(new File(url.getPath()));
                os.write(bytesOut.toByteArray());
                os.close();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @AfterClass
    public static void stopCluster() {
        if (isRunTestInVm()) {
            if (yarnCluster != null) {
                yarnCluster.stop();
            }
            if (dfsCluster != null) {
                dfsCluster.shutdown();
            }
        }
    }

    private BigDecimal round(final Double value, final int precision) {
        return new BigDecimal(value).round(new MathContext(precision));
    }

    public static boolean isRunTestInVm() {
        return runTestInVm;
    }

    public MongoClientURI getOutputUri() {
        return outputUri;
    }

    public MongoClientURI getInputUri() {
        return inputUri;
    }
}
