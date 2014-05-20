package com.mongodb.hadoop.hive;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer.HiveServerHandler;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.TreeMap;

import static java.lang.String.format;

public class HiveTest extends BaseHadoopTest {
    private static final Logger LOG = LoggerFactory.getLogger(HiveTest.class);

    private static HiveServer2 hiveServer;
//    private static CliDriver driver;
//    private static Client hiveClient;
//    private static SessionHandle sessionHandle;
//    private static HiveMetaStoreClient metaStoreClient;
    protected static HiveInterface client;

    @BeforeClass
    public static void startHive() throws TException, IOException {
        hiveServer = new HiveServer2();
        HiveConf hiveConf = new HiveConf();
//        hiveConf.addResource(getDfsCluster().getConfiguration(0));
        hiveConf.addResource(getYarnCluster().getConfig());
//        InputStream resourceAsStream = HiveTest.class.getResourceAsStream("/core-site.xml");
//        hiveConf.addResource(resourceAsStream);
        hiveConf.setVar(HiveConf.ConfVars.HADOOPBIN, HADOOP_HOME + "/bin/hadoop");
        MongoStorageHandler handler = new MongoStorageHandler();
        //        hiveConf.set("hive.server2.transport.mode", "socket");
        hiveServer.init(hiveConf);
        hiveServer.start();
        //        Collection<Service> services = hiveServer.getServices();
        //        Iterator<Service> iterator = services.iterator();
        //        CLIService cli = (CLIService) iterator.next();
        //        ThriftBinaryCLIService cliService = (ThriftBinaryCLIService) iterator.next();
/*
        try {
            //            sessionHandle = cli.openSession(null, null, new HashMap<String, String>());
            FileSystem.get(getYarnCluster().getConfig()).mkdirs(new Path("/user/hive/warehouse"));
        } catch (IOException e) {
            throw new RuntimeException(e);
            //        } catch (HiveSQLException e) {
            //            e.printStackTrace();
        }
*/
//        driver = new CliDriver();
//        final TSocket ts = new TSocket("localhost", 10000);
//                transport = TTransport.TBufferedTransport(ts)
//        Awaitility.await()
//                  .atMost(10, TimeUnit.SECONDS)
//                  .until(new Callable<Boolean>() {
//                      @Override
//                      public Boolean call() {
//                          try {
//                              ts.open();
//                              return true;
//                          } catch (TTransportException e) {
//                              return false;
//                          }
//                      }
//                  });
//        hiveClient = new Client(new Factory().getProtocol(ts));
//        metaStoreClient = new HiveMetaStoreClient(hiveConf);
        client = new HiveServerHandler(hiveConf);
        System.out.println(new TreeMap<Object, Object>(hiveConf.getAllProperties()));
    }

    @AfterClass
    public static void stopHive() {
        if (hiveServer != null) {
            hiveServer.stop();
        }
    }

//    public static Client getHiveClient() {
//        return hiveClient;
//    }

//    public static HiveMetaStoreClient getMetaStoreClient() {
//        return metaStoreClient;
//    }

//    public static CliDriver getDriver() {
//        return driver;
//    }

    @Override
    protected MongoClientURI getInputUri() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Test
    public void simpleJob() throws Exception {
                   dropTable("hive_test");
                   createHDFSHiveTable(TestHDFSToMongoDBTable.HIVE_TEST_TABLE, TestHDFSToMongoDBTable.TEST_SCHEMA, "\\t",
                                       TestHDFSToMongoDBTable.HIVE_TABLE_TYPE);
                   format("LOAD DATA LOCAL INPATH '%s'\nINTO TABLE 'hive_test'", getPath("test_data.txt"));
    }

    protected void createHDFSHiveTable(String name, String schema, String delimiter, String type) {
        LOG.info("strings = " + executeCommand(format("CREATE TABLE %s %s\n"
                                                     + "ROW FORMAT DELIMITED\n"
                                                     + "FIELDS TERMINATED BY '%s'\n"
                                                     + "STORED AS %s", name, schema, delimiter, type)));
    }

    protected String dropTable(final String tblName) {
        List<String> output = executeCommand(format("DROP TABLE %s", tblName));
        LOG.info("output = " + output);
        return null;

    }

/*
    protected void execute(String... commands) {
        for (String command : commands) {
            executeCommand(format("%s;\n", command.replace("\n", "\n\t")));
        }
    }
*/


    protected List<String> executeCommand(final String command) {
//         File scriptFile = null;
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info(format("Executing Hive command: %s", command));
            }
            client.execute(command);
            List<String> fetchAll = client.fetchAll();
            LOG.info("fetchAll = " + fetchAll);
            LOG.info("");
            LOG.info("");
            LOG.info("");
            return fetchAll;
/*
            scriptFile = File.createTempFile("test-", ".hive");
            scriptFile.deleteOnExit();
            final FileWriter scriptWriter = new FileWriter(scriptFile);
            scriptWriter.write(command);
            scriptWriter.flush();
            scriptWriter.close();
            if (LOG.isInfoEnabled()) {
                LOG.info(format("Executing Hive script:\n\n%s\n", command));
            }
            final Map<String, String> env = new TreeMap<String, String>(System.getenv());
            env.put("HADOOP_HOME", HADOOP_HOME);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ByteArrayOutputStream error = new ByteArrayOutputStream();
            new ProcessExecutor()
                .command(HIVE_HOME + "/bin/hive", "-f", scriptFile.getAbsolutePath())
                .environment(env)
                .redirectOutput(output)
                .redirectError(error)
                .execute();

            String stdout = new String(output.toByteArray());
            String stderr = new String(error.toByteArray());
            LOG.info("stdout = " + stdout);
            LOG.info("stderr = " + stderr);
            //            driver.run(new String[]{"-f", scriptFile.getAbsolutePath()});
*/
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
//        } finally {

/*
            if (scriptFile != null) {
                scriptFile.delete();
            }
*/
        }
    }


    protected String getPath(final String resource) {
        try {
            return new File(getClass().getResource("/" + resource).toURI()).getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
