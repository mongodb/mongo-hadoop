package org.mongodb.hadoop;

import com.mongodb.ReadPreference;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.streaming.io.MongoIdentifierResolver;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

import static com.mongodb.hadoop.util.MongoConfigUtil.INPUT_URI;
import static com.mongodb.hadoop.util.MongoConfigUtil.OUTPUT_URI;
import static java.lang.String.format;
import static org.mongodb.hadoop.MapReduceJob.HADOOP_HOME;
import static org.mongodb.hadoop.MapReduceJob.HADOOP_VERSION;

public class StreamingJob {
    private static final String STREAMING_JAR;
    private static final String STREAMING_MAPPER;

    private static final String STREAMING_REDUCER;

    private static final File STREAMING_HOME;

    static {
        try {
            File current = new File(".").getCanonicalFile();
            File home = new File(current, "streaming");
            while (!home.exists() && current.getParentFile().exists()) {
                current = current.getParentFile();
                home = new File(current, "streaming");
            }
            STREAMING_HOME = home;
            STREAMING_JAR = new File(STREAMING_HOME, "build/libs").listFiles(new HadoopVersionFilter())[0].getAbsolutePath();
            STREAMING_MAPPER = new File(STREAMING_HOME, "examples/treasury/mapper.py").getAbsolutePath();
            STREAMING_REDUCER = new File(STREAMING_HOME, "examples/treasury/reducer.py").getAbsolutePath();
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static final Log LOG = LogFactory.getLog(StreamingJob.class);

    private List<String> cmd = new ArrayList<String>();

    private String hostName = "localhost:27017";
    private ReadPreference readPreference = ReadPreference.primary();

    private String inputAuth = null;
    private String inputCollection = "mongo_hadoop.yield_historical.in";
    private String inputFormat = MongoInputFormat.class.getName();
    private String inputPath = format("file://%s/in", System.getProperty("java.io.tmpdir"));

    private String outputAuth = null;
    private String outputCollection = "mongo_hadoop.yield_historical.out";
    private String outputFormat = MongoOutputFormat.class.getName();
    private String outputPath = format("file://%s/out", System.getProperty("java.io.tmpdir"));
    private Map<String, String> params;

    public StreamingJob() {
        cmd.add(BaseHadoopTest.HADOOP_HOME + "/bin/hadoop");
        cmd.add("jar");
        if (BaseHadoopTest.HADOOP_VERSION.startsWith("1.1")) {
            cmd.add(String.format("%s/contrib/streaming/hadoop-streaming-%s.jar",
                                  BaseHadoopTest.HADOOP_HOME,
                                  BaseHadoopTest.HADOOP_RELEASE_VERSION));
        } else {
            cmd.add(String.format("%s/share/hadoop/tools/lib/hadoop-streaming-%s.jar",
                                  BaseHadoopTest.HADOOP_HOME,
                                  BaseHadoopTest.HADOOP_RELEASE_VERSION));
        }
//        add("-libjars", STREAMING_JAR);
        add("-io", "mongodb");
        add("-jobconf", "stream.io.identifier.resolver.class=" + MongoIdentifierResolver.class.getName());
        add("-mapper", STREAMING_MAPPER);
        add("-reducer", STREAMING_REDUCER);
    }

    public StreamingJob hostName(final String hostName) {
        this.hostName = hostName;
        return this;
    }

    public StreamingJob readPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    public StreamingJob inputAuth(final String inputAuth) {
        this.inputAuth = inputAuth;
        return this;
    }

    public StreamingJob inputCollection(final String inputCollection) {
        this.inputCollection = inputCollection;
        return this;
    }

    public StreamingJob inputFormat(final String format) {
        inputFormat = format;
        return this;
    }

    public StreamingJob inputPath(final String path) {
        inputPath = path;
        return this;
    }

    public StreamingJob outputAuth(final String outputAuth) {
        this.outputAuth = outputAuth;
        return this;
    }

    public StreamingJob outputCollection(final String outputCollection) {
        this.outputCollection = outputCollection;
        return this;
    }

    public StreamingJob outputFormat(final String format) {
        outputFormat = format;
        return this;
    }

    public StreamingJob outputPath(final String path) {
        outputPath = path;
        return this;
    }

    public StreamingJob params(final Map<String, String> params) {
        this.params = params;
        return this;
    }

    public void execute() {
        try {
            copyJars();
            add("-input", inputPath);
            add("-output", outputPath);
            add("-inputformat", inputFormat);
            add("-outputformat", outputFormat);

            add("-jobconf", format("%s=mongodb://%s%s/%s?readPreference=%s", INPUT_URI, inputAuth != null ? inputAuth + "@" : "",
                                   hostName, inputCollection, readPreference));

            add("-jobconf", format("%s=mongodb://%s%s/%s", OUTPUT_URI, outputAuth != null ? outputAuth + "@" : "", hostName,
                                   outputCollection));

            for (final Entry<String, String> entry : params.entrySet()) {
                add("-jobconf", entry.getKey() + "=" + entry.getValue());

            }
            final Map<String, String> env = new TreeMap<String, String>(System.getenv());
            if (BaseHadoopTest.HADOOP_VERSION.startsWith("cdh")) {
                env.put("MAPRED_DIR", "share/hadoop/mapreduce2");
            }

            LOG.info("Executing hadoop job:");

            final StringBuilder output = new StringBuilder();
            final Iterator<String> iterator = cmd.iterator();
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
                                 .environment(env)
                                 .redirectError(System.out)
                                 .execute();

            Thread.sleep(5000);  // let the system settle
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (final TimeoutException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }

    private void add(final String flag, final String value) {
        cmd.add(flag);
        cmd.add(value);
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
            
            File coreJar = new File("../core/build/libs").listFiles(new HadoopVersionFilter())[0];
            FileUtils.copyFile(coreJar, new File(hadoopLib, "mongo-hadoop-core.jar"));
            
            File mongoStreamingJar = new File("../streaming/build/libs").listFiles(new HadoopVersionFilter())[0];
            FileUtils.copyFile(mongoStreamingJar, new File(hadoopLib, "mongo-hadoop-streaming.jar"));
            
            File hadoopStreamingJar = new File(hadoopLib + "/../tools/lib").listFiles(new FilenameFilter() {
                @Override
                public boolean accept(final File dir, final String name) {
                    return name.startsWith("hadoop-streaming-");
                }
            })[0];
            FileUtils.copyFile(hadoopStreamingJar, new File(hadoopLib, hadoopStreamingJar.getName()));

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    
}
