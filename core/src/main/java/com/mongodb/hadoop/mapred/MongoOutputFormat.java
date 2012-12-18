// MongoOutputFormat.java
/*
 * Copyright 2010 10gen Inc.
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

import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

import com.mongodb.hadoop.mapred.output.*;
import com.mongodb.hadoop.util.*;

@SuppressWarnings("deprecation")
public class MongoOutputFormat<K, V> implements OutputFormat<K, V> {
    private static final Log log = LogFactory.getLog(MongoOutputFormat.class);

    public MongoOutputFormat() {
    }

    public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
        return new MongoOutputCommiter();
    }

    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) {
        return new MongoRecordWriter(MongoConfigUtil.getOutputCollections(job), job);
    }

}
