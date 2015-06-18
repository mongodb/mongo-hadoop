package com.mongodb.hadoop.testutils;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.HadoopVersionFilter;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public abstract class BaseHadoopTest {
    private static final Logger LOG = LoggerFactory.getLogger(BaseHadoopTest.class);

    public static final String HADOOP_HOME;
    public static final String PROJECT_VERSION = loadProperty("project"
            + ".version", "1.4-rc1-SNAPSHOT");
    public static final String HADOOP_VERSION = loadProperty("hadoop.version", "2.6.0");

//    public static final String HIVE_HOME;
    public static final File PROJECT_HOME;
    public static final String HADOOP_BINARIES;
    public static final String EXAMPLE_DATA_HOME;

    private static final boolean TEST_IN_VM = Boolean.valueOf(System.getProperty("mongo.hadoop.testInVM", "false"));

    private static final String MONGO_IMPORT;

    private MongoClient client;

    static {
        try {
            File current = new File(".").getCanonicalFile();
            while (!new File(current, "build.gradle").exists() && current.getParentFile().exists()) {
                current = current.getParentFile();
            }
            PROJECT_HOME = current;

            String property = System.getProperty("mongodb_server");
            String serverType = property != null ? property.replaceAll("-release", "") : "UNKNOWN";
            if (serverType.equals("27-nightly")) {
                serverType = "master-nightly";
                property = serverType + "-release";
            }
            final String path = format("/mnt/jenkins/mongodb/%s/%s/bin/mongoimport", serverType, property);
            MONGO_IMPORT = new File(path).exists() ? path : "/usr/local/bin/mongoimport";
            if (!new File(MONGO_IMPORT).exists()) {
                throw new RuntimeException(format("Can not locate mongoimport.  Tried looking in '%s' and '%s' assuming a server "
                                                  + "type of '%s'", path, "/usr/local/bin/mongoimport", property));
            }

            final File gradleProps = new File(PROJECT_HOME, ".gradle.properties");
            if (gradleProps.exists()) {
                System.getProperties().load(new FileInputStream(gradleProps));
            }
            HADOOP_BINARIES = new File(PROJECT_HOME, "hadoop-binaries/").getCanonicalPath();
            EXAMPLE_DATA_HOME = new File(HADOOP_BINARIES, "examples/data").getCanonicalPath();

            HADOOP_HOME = new File(HADOOP_BINARIES, format("hadoop-%s", HADOOP_VERSION)).getCanonicalPath();
//            HIVE_HOME = new File(System.getProperty("hive_home")).getCanonicalPath();
            LOG.info("HADOOP_HOME = " + HADOOP_HOME);
//            LOG.info("HIVE_HOME = " + HIVE_HOME);

        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected static String loadProperty(final String name, final String defaultValue) {
        String property = System.getProperty(name, System.getenv(name.toUpperCase()));
        if (property == null) {
            property = defaultValue;
        }
        return property;
    }


    protected static DBObject dbObject(final Object... values) {
        final BasicDBObject object = new BasicDBObject();
        for (int i = 0; i < values.length; i += 2) {
            object.append(values[i].toString(), values[i + 1]);
        }
        return object;
    }

    public static MongoClientURIBuilder authCheck(final MongoClientURIBuilder builder) {
        if (isAuthEnabled()) {
            builder.auth("bob", "pwd123");
        }

        return builder;
    }

    protected static File findProjectJar(final File root) {
        return findProjectJar(root, false);
    }

    protected static File findProjectJar(final File root, final boolean findTestJar) {
        try {
            File current = new File(".").getCanonicalFile();
            File core = new File(current, "core");
            while (!core.exists() && current.getParentFile().exists()) {
                current = current.getParentFile();
                core = new File(current, "core");
            }

            final File file = new File(root, "build/libs").getCanonicalFile();
            final File[] files = file.listFiles(new HadoopVersionFilter(findTestJar));
            if (files.length == 0) {
                throw new RuntimeException(format("Can't find jar.  project version = %s, path = %s, findTestJar = %s",
                                                  PROJECT_VERSION, file, findTestJar));
            }
            return files[0];
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static boolean isHadoopV1() {
        return BaseHadoopTest.HADOOP_VERSION.startsWith("1.");
    }

    public void mongoImport(final String collection, final File file) {
        try {
            final List<String> command = new ArrayList<String>();
            command.addAll(asList(MONGO_IMPORT,
                                  "--drop",
                                  "--db", "mongo_hadoop",
                                  "--collection", collection,
                                  "--file", file.getAbsolutePath()));
            if (isAuthEnabled()) {
                final List<String> list = new ArrayList<String>(asList("-u", "bob",
                                                                 "-p", "pwd123"));
                if (!System.getProperty("mongodb_server", "").equals("22-release")) {
                    list.addAll(asList("--authenticationDatabase", "admin"));
                }
                command.addAll(list);
            }
            final StringBuilder output = new StringBuilder();
            final Iterator<String> iterator = command.iterator();
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

            final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            final ByteArrayOutputStream errStream = new ByteArrayOutputStream();
            final ProcessExecutor executor = new ProcessExecutor().command(command)
                                                            .readOutput(true)
                                                            .redirectOutput(outStream)
                                                            .redirectError(errStream);
            final ProcessResult result = executor.execute();
            if (result.getExitValue() != 0) {
                LOG.error(result.getOutput().getString());
                throw new RuntimeException(String.format("mongoimport failed with exit code %d: %s", result.getExitValue(),
                                                         result.getOutput().getString()));
            }

        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (final TimeoutException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public MongoClient getClient(final MongoClientURI uri) {
        if (client == null) {
            try {
                client = new MongoClient(uri);
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return client;
    }

    protected List<DBObject> toList(final DBCursor cursor) {
        final List<DBObject> list = new ArrayList<DBObject>();
        while (cursor.hasNext()) {
            list.add(cursor.next());
        }

        return list;
    }

    protected static boolean isAuthEnabled() {
        return Boolean.valueOf(System.getProperty("authEnabled", "false"))
               || "auth".equals(System.getProperty("mongodb_option"));
    }

    protected boolean isSharded(final MongoClientURI uri) {
        final CommandResult isMasterResult = runIsMaster(uri);
        final Object msg = isMasterResult.get("msg");
        return msg != null && msg.equals("isdbgrid");
    }

    protected CommandResult runIsMaster(final MongoClientURI uri) {
        // Check to see if this is a replica set... if not, get out of here.
        return getClient(uri).getDB("admin").command(new BasicDBObject("ismaster", 1));
    }

    public static boolean isRunTestInVm() {
        return TEST_IN_VM;
    }
}
