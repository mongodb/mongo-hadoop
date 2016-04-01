package com.mongodb.hadoop;

import com.mongodb.hadoop.output.MongoOutputCommitter;
import com.mongodb.hadoop.util.CompatUtils;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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

}
