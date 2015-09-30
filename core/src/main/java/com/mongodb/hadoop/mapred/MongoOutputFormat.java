/*
 * Copyright 2010-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.mapred;

import com.mongodb.hadoop.mapred.output.MongoOutputCommitter;
import com.mongodb.hadoop.mapred.output.MongoRecordWriter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class MongoOutputFormat<K, V> implements OutputFormat<K, V> {
    public MongoOutputFormat() {
    }

    @Override
    public void checkOutputSpecs(final FileSystem ignored, final JobConf job) throws IOException {
        if (MongoConfigUtil.getOutputURIs(job).isEmpty()) {
            throw new IOException("No output URI is specified. You must set mongo.output.uri.");
        }
    }

    /**
     * @deprecated This method is unused.
     * @param context the current task's context.
     * @return an instance of {@link MongoOutputCommitter}
     */
    @Deprecated
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) {
        return new MongoOutputCommitter();
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(
      final FileSystem ignored, final JobConf job, final String name,
      final Progressable progress) {
        return new MongoRecordWriter<K, V>(job);
    }

}
