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


package com.mongodb.hadoop.mapred.output;

import com.mongodb.DBCollection;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;

import java.util.Collections;
import java.util.List;

public class MongoRecordWriter<K, V>
  extends com.mongodb.hadoop.output.MongoRecordWriter<K, V>
  implements RecordWriter<K, V> {
    private final JobConf configuration;

    /**
     * Create a new MongoRecordWriter.
     * @param conf the job configuration
     */
    public MongoRecordWriter(final JobConf conf) {
        super(
          Collections.<DBCollection>emptyList(),
          new TaskAttemptContextImpl(
            conf, TaskAttemptID.forName(conf.get("mapred.task.id"))));
        configuration = conf;
    }

    /**
     * @deprecated MongoRecordWriter doesn't use DBCollections directly.
     * Please use {@link #MongoRecordWriter(JobConf)} instead.
     * @param c the DBCollection
     * @param conf the job configuration
     */
    @Deprecated
    public MongoRecordWriter(final List<DBCollection> c, final JobConf conf) {
        this(conf);
    }

    @Override
    public void close(final Reporter reporter) {
        super.close(getContext());
    }

    public JobConf getConf() {
        return configuration;
    }

}
