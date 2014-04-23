package org.mongodb.hadoop;

import com.mongodb.ReadPreference;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static org.mongodb.hadoop.BaseHadoopTest.loadProperty;

public class MapReduceJob {
    private static final Logger LOG = LoggerFactory.getLogger(MapReduceJob.class);

    public static final String HADOOP_HOME;
    public static final String HADOOP_VERSION = loadProperty("hadoop.version", "2.3");
    public static final String HADOOP_RELEASE_VERSION = loadProperty("hadoop.release.version", "2.3.0");

    public static final File TREASURY_YIELD_HOME;
    public static final File JSONFILE_PATH;

    protected static final File JOBJAR_PATH;

    static {
        try {
            HADOOP_HOME = new File(format("%s/hadoop-binaries/hadoop-%s", System.getProperty("user.home"),
                                          HADOOP_RELEASE_VERSION)).getCanonicalPath();

            File current = new File(".").getCanonicalFile();
            File home = new File(current, "examples/treasury_yield");
            while (!home.exists() && current.getParentFile().exists()) {
                current = current.getParentFile();
                home = new File(current, "examples/treasury_yield");
            }
            TREASURY_YIELD_HOME = home;
            JSONFILE_PATH = new File(TREASURY_YIELD_HOME, "/src/main/resources/yield_historical_in.json");

            File file = new File(TREASURY_YIELD_HOME, "build/libs").getCanonicalFile();
            File[] files = file.listFiles(new HadoopVersionFilter());
            if (files.length == 0) {
                throw new RuntimeException(format("Can't find jar.  hadoop version = %s, path = %s", HADOOP_VERSION, file));
            }
            JOBJAR_PATH = files[0];
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Map<String, String> params = new LinkedHashMap<String, String>();
    private final String className;

    private String inputAuth = null;
    private final List<String> inputCollections = new ArrayList<String>();
    private final List<String> inputUris = new ArrayList<String>();

    private final List<String> outputUris = new ArrayList<String>();
    private String outputAuth = null;
    private ReadPreference readPreference = ReadPreference.primary();
    private String host = "localhost";
    private int port = 27017;

    public MapReduceJob(final Class<? extends Tool> toolClass) {
        this.className = toolClass.getName();
    }

    public MapReduceJob host(final String host) {
        this.host = host;
        return this;
    }

    public MapReduceJob port(final int port) {
        this.port = port;
        return this;
    }

    public MapReduceJob inputAuth(final String inputAuth) {
        this.inputAuth = inputAuth;
        return this;
    }

    public MapReduceJob outputAuth(final String outputAuth) {
        this.outputAuth = outputAuth;
        return this;
    }

    public MapReduceJob readPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    public MapReduceJob param(final String key, final String value) {
        params.put(key, value);
        return this;
    }

    public MapReduceJob inputCollections(final String... inputCollections) {
        this.inputCollections.addAll(Arrays.asList(inputCollections));
        return this;
    }

    public MapReduceJob inputUris(final String... inputUris) {
        this.inputUris.addAll(Arrays.asList(inputUris));
        return this;
    }

    public MapReduceJob outputUris(final String... outputUris) {
        this.outputUris.addAll(Arrays.asList(outputUris));
        return this;
    }

    public void execute(final boolean inVM) {
        try {
            copyJars();
            if (inVM) {
                executeInVM();
            } else {
                executeExternal();
            }
            Thread.sleep(5000);  // let the system settle
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void executeExternal() throws IOException, TimeoutException, InterruptedException {
        List<String> cmd = new ArrayList<String>();
        cmd.add(new File(HADOOP_HOME, "bin/hadoop").getCanonicalPath());
        cmd.add("jar");
        cmd.add(JOBJAR_PATH.getAbsolutePath());
        cmd.add(className);

        for (Pair<String, String> entry : processSettings()) {
            cmd.add(format("-D%s=%s", entry.getKey(), entry.getValue()));
        }

        Map<String, String> env = new TreeMap<String, String>(System.getenv());
        if (HADOOP_VERSION.startsWith("cdh")) {
            env.put("MAPRED_DIR", "share/hadoop/mapreduce2");
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

        LOG.info(output.toString());
        new ProcessExecutor().command(cmd)
                             .environment(env)
                             .redirectError(System.out)
                             .execute();

    }

    public void executeInVM() throws Exception {
        List<String> cmd = new ArrayList<String>();
        for (Pair<String, String> entry : processSettings()) {
            cmd.add(format("-D%s=%s", entry.getKey(), entry.getValue()));
        }
        Map<String, String> env = new TreeMap<String, String>(System.getenv());
        if (HADOOP_VERSION.startsWith("cdh")) {
            env.put("MAPRED_DIR", "share/hadoop/mapreduce2");
            System.setProperty("MAPRED_DIR", "share/hadoop/mapreduce2");
        }

        LOG.info("Executing hadoop job");

        ToolRunner.run((org.apache.hadoop.util.Tool) Class.forName(className).newInstance(), cmd.toArray(new String[cmd.size()]));
    }

    private List<Pair<String, String>> processSettings() {
        List<Pair<String, String>> entries = new ArrayList<Pair<String, String>>();
        for (Entry<String, String> entry : params.entrySet()) {
            entries.add(new Pair<String, String>(entry.getKey(), entry.getValue()));
        }

        if (!inputCollections.isEmpty()) {
            StringBuilder inputUri = new StringBuilder();
            for (String collection : inputCollections) {
                if (inputUri.length() != 0) {
                    inputUri.append(",");
                }
                inputUri.append(format("mongodb://%s:%d/%s?readPreference=%s", host, port, collection, readPreference));
            }
            entries.add(new Pair<String, String>(MongoConfigUtil.INPUT_URI, inputUri.toString()));
        } else if (!inputUris.isEmpty()) {
            StringBuilder inputUri = new StringBuilder();
            for (String uri : inputUris) {
                if (inputUri.length() != 0) {
                    inputUri.append(",");
                }
                inputUri.append(uri);
            }
            entries.add(new Pair<String, String>(MongoConfigUtil.INPUT_URI, inputUri.toString()));
        } else if (readPreference != ReadPreference.primary()) {
            entries.add(new Pair<String, String>(MongoConfigUtil.INPUT_URI, format("mongodb://%s:%d/%s?readPreference=%s", host, port,
                                                                                   "yield_historical.in", readPreference)));
        }
        if (!outputUris.isEmpty()) {
            StringBuilder outputUri = new StringBuilder();
            for (String uri : outputUris) {
                if (outputUri.length() != 0) {
                    outputUri.append(",");
                }
                outputUri.append(uri);
            }
            entries.add(new Pair<String, String>(MongoConfigUtil.OUTPUT_URI, outputUri.toString()));
        }

        return entries;
    }

    private void copyJars() {
        String hadoopLib = format(HADOOP_VERSION.startsWith("1") ? HADOOP_HOME + "/lib"
                                                                 : HADOOP_HOME + "/share/hadoop/common");
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

    private static class Pair<T, U> {
        private T key;
        private U value;

        private Pair(final T key, final U value) {
            this.key = key;
            this.value = value;
        }

        public T getKey() {
            return key;
        }

        public U getValue() {
            return value;
        }
    }
}
