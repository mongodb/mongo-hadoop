package com.mongodb.hadoop.testutils;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.HadoopVersionFilter;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

import static com.mongodb.hadoop.testutils.BaseHadoopTest.HADOOP_HOME;
import static com.mongodb.hadoop.testutils.BaseHadoopTest.HADOOP_VERSION;
import static com.mongodb.hadoop.testutils.BaseHadoopTest.PROJECT_HOME;
import static com.mongodb.hadoop.testutils.BaseHadoopTest.isHadoopV1;
import static com.mongodb.hadoop.util.MongoConfigUtil.INPUT_URI;
import static com.mongodb.hadoop.util.MongoConfigUtil.JOB_INPUT_FORMAT;
import static com.mongodb.hadoop.util.MongoConfigUtil.JOB_OUTPUT_FORMAT;
import static com.mongodb.hadoop.util.MongoConfigUtil.OUTPUT_URI;
import static java.lang.String.format;

public class MapReduceJob {
    private static final Logger LOG = LoggerFactory.getLogger(MapReduceJob.class);

    private Map<String, String> params = new LinkedHashMap<String, String>();
    private final String className;

    private final List<String> inputUris = new ArrayList<String>();
    private final List<String> outputUris = new ArrayList<String>();
    private File jarPath;
    private Class<? extends InputFormat> inputFormat;
    private Class<? extends OutputFormat> outputFormat;
    private Class<? extends org.apache.hadoop.mapred.InputFormat> mapredInputFormat;
    private Class<? extends org.apache.hadoop.mapred.OutputFormat> mapredOutputFormat;

    public MapReduceJob(final String className) {
        this.className = className;
    }

    public MapReduceJob param(final String key, final String value) {
        params.put(key, value);
        return this;
    }

    public MapReduceJob inputUris(final MongoClientURI... inputUris) {
        for (MongoClientURI inputUri : inputUris) {
            this.inputUris.add(inputUri.getURI());
        }
        return this;
    }

    public MapReduceJob outputUris(final MongoClientURI... outputUris) {
        for (MongoClientURI outputUri : outputUris) {
            this.outputUris.add(outputUri.getURI());
        }
        return this;
    }

    public MapReduceJob inputUris(final URI... inputUris) {
        for (URI inputUri : inputUris) {
            this.inputUris.add(inputUri.toString());
        }
        return this;
    }

    public MapReduceJob outputUris(final URI... outputUris) {
        for (URI outputUri : outputUris) {
            this.outputUris.add(outputUri.toString());
        }
        return this;
    }

    public MapReduceJob jar(final File path) {
        jarPath = path;
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
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void executeExternal() throws IOException, TimeoutException, InterruptedException {
        List<String> cmd = new ArrayList<String>();
        cmd.add(new File(HADOOP_HOME, "bin/hadoop").getCanonicalPath());
        cmd.add("jar");
        cmd.add(jarPath.getAbsolutePath());
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

    @SuppressWarnings("unchecked")
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

        Class<? extends MongoTool> jobClass = (Class<? extends MongoTool>) Class.forName(className);
        Configuration conf = new Configuration();
        MongoTool app = (MongoTool) jobClass.getConstructor(new Class[]{Configuration.class})
                                .newInstance(conf);

        ToolRunner.run(conf, app, cmd.toArray(new String[cmd.size()]));
    }

    private List<Pair<String, String>> processSettings() {
        List<Pair<String, String>> entries = new ArrayList<Pair<String, String>>();
        for (Entry<String, String> entry : params.entrySet()) {
            entries.add(new Pair<String, String>(entry.getKey(), entry.getValue()));
        }

        StringBuilder inputUri = new StringBuilder();
        if (!inputUris.isEmpty()) {
            for (String uri : inputUris) {
                if (inputUri.length() != 0) {
                    inputUri.append(",");
                }
                inputUri.append(uri);
            }
            entries.add(new Pair<String, String>(INPUT_URI, inputUri.toString()));
        }

        if (!outputUris.isEmpty()) {
            StringBuilder outputUri = new StringBuilder();
            for (String uri : outputUris) {
                if (outputUri.length() != 0) {
                    outputUri.append(",");
                }
                outputUri.append(uri);
            }
            entries.add(new Pair<String, String>(OUTPUT_URI, outputUri.toString()));
        }
        
        if (inputFormat != null) {
            entries.add(new Pair<String, String>(JOB_INPUT_FORMAT, inputFormat.getName()));
        } else if (mapredInputFormat != null) {
            entries.add(new Pair<String, String>(JOB_INPUT_FORMAT, mapredInputFormat.getName()));
        } else {
            String name;
            if (BaseHadoopTest.isHadoopV1()) {
                name = com.mongodb.hadoop.mapred.MongoInputFormat.class.getName();
            } else {
                name = MongoInputFormat.class.getName();
            }
            entries.add(new Pair<String, String>(JOB_INPUT_FORMAT, name));
            LOG.info("No input format defined.  Defaulting to '%s'", name);
        }

        if (outputFormat != null) {
            LOG.info("Adding output format '%s'", outputFormat.getName());
            entries.add(new Pair<String, String>(JOB_OUTPUT_FORMAT, outputFormat.getName()));
        } else if (mapredOutputFormat != null) {
            LOG.info("Adding output format '%s'", mapredOutputFormat.getName());
            entries.add(new Pair<String, String>(JOB_OUTPUT_FORMAT, mapredOutputFormat.getName()));
        } else {
            String name = isHadoopV1()
                          ? com.mongodb.hadoop.mapred.MongoOutputFormat.class.getName()
                          : MongoOutputFormat.class.getName();
            entries.add(new Pair<String, String>(JOB_OUTPUT_FORMAT, name));
            LOG.info(format("No output format defined.  Defaulting to '%s'", name));
        }

        return entries;
    }

    private void copyJars() {
        String hadoopLib = format(isHadoopV1() ? HADOOP_HOME + "/lib" : HADOOP_HOME + "/share/hadoop/common");
        try {
            URLClassLoader classLoader = (URLClassLoader) getClass().getClassLoader();
            for (URL url : classLoader.getURLs()) {
                boolean contains = url.getPath().contains("mongo-java-driver");
                if (contains) {
                    File file = new File(url.toURI());
                    FileUtils.copyFile(file, new File(hadoopLib, "mongo-java-driver.jar"));
                }
            }
            File coreJar = new File(PROJECT_HOME, "core/build/libs").listFiles(new HadoopVersionFilter())[0];
            FileUtils.copyFile(coreJar, new File(hadoopLib, "mongo-hadoop-core.jar"));

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public MapReduceJob inputFormat(final Class<? extends InputFormat> inputFormat) {
        this.inputFormat = inputFormat;
        return this;
    }

    public MapReduceJob outputFormat(final Class<? extends OutputFormat> outputFormat) {
        this.outputFormat = outputFormat;
        return this;
    }

    public MapReduceJob mapredInputFormat(final Class<? extends org.apache.hadoop.mapred.InputFormat> inputFormat) {
        this.mapredInputFormat = inputFormat;
        return this;
    }

    public MapReduceJob mapredOutputFormat(final Class<? extends org.apache.hadoop.mapred.OutputFormat> outputFormat) {
        this.mapredOutputFormat = outputFormat;
        return this;
    }

    private static final class Pair<T, U> {
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

        @Override
        public String toString() {
            return String.format("Pair{key=%s, value=%s}", key, value);
        }
    }
}
