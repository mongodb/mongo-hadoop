package com.mongodb.hadoop.pig;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.input.BSONFileRecordReader;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

public class PigTest extends BaseHadoopTest {
    private static final Logger LOG = LoggerFactory.getLogger(PigTest.class);

    public static final String PIG;

    private static final String PIG_HOME;
    public static final String PIG_VERSION = loadProperty("pig.version", "0.13.0");

    static {
        try {
            PIG_HOME = new File(HADOOP_BINARIES, format("pig-%s/", PIG_VERSION)).getCanonicalPath();
            String os = System.getProperty("os.name").toLowerCase();
            PIG = new File(PIG_HOME, "bin/pig" + (os.contains("windows") ? ".cmd" : "")).getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }

    private MongoClientURI uri;
    private MongoClient mongoClient;

    @Before
    public void setup() throws UnknownHostException {
        uri = new MongoClientURI("mongodb://localhost:27017/mongo_hadoop.pigtests");
        mongoClient = new MongoClient(uri);
    }

    @After
    public void tearDown() {
        mongoClient.close();
    }

    @Test
    public void mongoUpdateStorage() throws InterruptedException, TimeoutException, IOException {
        BSONFileRecordReader reader = new BSONFileRecordReader();
        File file = new File(PROJECT_HOME + "/pig/src/test/resources/dump/test/persons_info.bson");
        FileSplit split = new FileSplit(new Path(file.toURI()), 0L, file.length(), new String[0]);
        reader.initialize(split, new TaskAttemptContextImpl(new JobConf(), new TaskAttemptID()));
        while (reader.nextKeyValue()) {
            LOG.info(reader.getCurrentValue().toString());
        }
        runScript("update_simple_mus.pig");
    }

    public void runScript(final String script) throws IOException, TimeoutException, InterruptedException {
        List<String> cmd = new ArrayList<String>();
        cmd.add(PIG);
        cmd.add("-x");
        cmd.add("mapreduce");
        cmd.add("-l");
        cmd.add(format("%s/pig/build", PROJECT_HOME));
        cmd.add(format("%s/pig/build/resources/test/pig/%s", PROJECT_HOME, script));

        LOG.info("Executing pig script:");

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
        final Map<String, String> env = new HashMap<String, String>();
        String path = System.getenv("PATH");
        env.put("HADOOP_HOME", HADOOP_HOME);
        env.put("path", HADOOP_HOME + "/bin" + File.pathSeparator + path);
        new ProcessExecutor().command(cmd)
                             .environment(env)
                             .redirectError(System.out)
                             .execute();

    }

}
