package com.mongodb.hadoop;

import com.mongodb.hadoop.output.MongoOutputCommitter;
import com.mongodb.hadoop.util.CompatUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoOutputCommitterTest {

    @Test
    public void testGetTaskAttemptPath() {
        // Empty configuration.
        JobConf conf = new JobConf();
        String taskName = "attempt_local138413205_0007_m_000000_0";
        String suffix = String.format(
          "/%s/%s/_out", taskName, MongoOutputCommitter.TEMP_DIR_NAME);
        CompatUtils.TaskAttemptContext context =
          CompatUtils.getTaskAttemptContext(conf, taskName);
        conf.clear();

        // /tmp
        assertEquals(
          "/tmp" + suffix,
          MongoOutputCommitter.getTaskAttemptPath(context).toUri().getPath());

        // system-wide tmp dir
        conf.set("hadoop.tmp.dir", "/system-wide");
        assertEquals(
          "/system-wide" + suffix,
          MongoOutputCommitter.getTaskAttemptPath(context).toUri().getPath());

        // old style option
        conf.set("mapred.child.tmp", "/child-tmp");
        assertEquals(
          "/child-tmp" + suffix,
          MongoOutputCommitter.getTaskAttemptPath(context).toUri().getPath());

        // new style option
        conf.set("mapreduce.task.tmp.dir", "/new-child-tmp");
        assertEquals(
          "/new-child-tmp" + suffix,
          MongoOutputCommitter.getTaskAttemptPath(context).toUri().getPath());
    }

    @Test
    public void testCleanupResources() throws IOException {
        // Empty configuration.
        JobConf conf = new JobConf();
        String taskName = "attempt_local138413205_0007_m_000000_0";
        CompatUtils.TaskAttemptContext context =
          CompatUtils.getTaskAttemptContext(conf, taskName);
        conf.clear();
        conf.set("mapreduce.task.tmp.dir", "/tmp");
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        Path taskAttemptDir = MongoOutputCommitter.getTaskAttemptPath(context);
        FileSystem fs = FileSystem.getLocal(conf);

        fs.create(taskAttemptDir);

        MongoOutputCommitter committer = new MongoOutputCommitter();

        // Trigger cleanupResources.
        committer.abortTask(context);

        assertFalse(fs.exists(taskAttemptDir));
        assertFalse(fs.exists(new Path("/tmp/" + taskName)));
        assertTrue(fs.exists(new Path("/tmp")));
    }

}
