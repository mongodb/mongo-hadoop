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

import com.mongodb.MongoURI;
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
import java.util.List;

@SuppressWarnings("deprecation")
public class MongoOutputFormat<K, V> implements OutputFormat<K, V> {

    private final String[] updateKeys;
    private final boolean multiUpdate;
    
    public MongoOutputFormat() {
        this(null, false);
    }
    
    public MongoOutputFormat(String[] updateKeys, boolean multiUpdate) {
        this.updateKeys = updateKeys;
        this.multiUpdate = multiUpdate;
    }

    public void checkOutputSpecs(final FileSystem ignored, final JobConf job) throws IOException {
        List<MongoURI> outputUris;
        outputUris = MongoConfigUtil.getOutputURIs(job);
        if (outputUris == null || outputUris.size() == 0) {
            throw new IOException("No output URI is specified. You must set mongo.output.uri.");
        }
    }

    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) {
        return new MongoOutputCommitter();
    }

    public RecordWriter<K, V> getRecordWriter(final FileSystem ignored, final JobConf job, final String name,
                                              final Progressable progress) {
        return new MongoRecordWriter<K, V>(MongoConfigUtil.getOutputCollections(job), job, updateKeys, multiUpdate);
    }

}
