package com.mongodb.hadoop.testutils;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
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
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public abstract class BaseHadoopTest {
    private static final Logger LOG = LoggerFactory.getLogger(BaseHadoopTest.class);

    public static final String HADOOP_HOME;
    public static final String HADOOP_VERSION = loadProperty("hadoop.version", "2.4");
    public static final String HADOOP_RELEASE_VERSION = loadProperty("hadoop.release.version", "2.4.0");
    public static final String HIVE_HOME;
    public static final String HIVE_VERSION = "0.12.0";
    public static final File PROJECT_HOME;

    private static final boolean TEST_IN_VM = Boolean.valueOf(System.getProperty("mongo.hadoop.testInVM", "false"));

    private static MiniYARNCluster yarnCluster;

    private static MiniDFSCluster dfsCluster;

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

            HADOOP_HOME = new File(String.format("%s/hadoop-binaries/hadoop-%s/", System.getProperty("user.home"),
                                                 HADOOP_RELEASE_VERSION)).getCanonicalPath();
            HIVE_HOME = new File(String.format("%s/hadoop-binaries/hive-%s/", System.getProperty("user.home"),
                                               HIVE_VERSION)).getCanonicalPath();

            File current = new File(".").getCanonicalFile();
            while (!new File(current, "build.gradle").exists() && current.getParentFile().exists()) {
                current = current.getParentFile();
            }
            PROJECT_HOME = current;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static MiniYARNCluster getYarnCluster() {
        if (yarnCluster == null) {
            setupCluster();
        }
        return yarnCluster;
    }

    public static MiniDFSCluster getDfsCluster() {
        if (dfsCluster == null) {
            setupCluster();
        }
        return dfsCluster;
    }

    @BeforeClass
    public static void setupCluster() {
        if (isRunTestInVm() && yarnCluster == null && dfsCluster == null) {
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
                yarnCluster = new MiniYARNCluster("mongo-hadoop", 1, 1, 1);
                yarnCluster.init(config);

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


    protected abstract MongoClientURI getInputUri();

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

    protected List<DBObject> toList(final DBCursor cursor) {
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

    public static boolean isRunTestInVm() {
        return TEST_IN_VM;
    }
}
