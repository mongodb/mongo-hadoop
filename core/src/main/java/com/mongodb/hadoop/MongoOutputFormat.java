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

package com.mongodb.hadoop;

// Mongo

import com.mongodb.MongoURI;
import com.mongodb.hadoop.output.MongoOutputCommitter;
import com.mongodb.hadoop.output.MongoRecordWriter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class MongoOutputFormat<K, V> extends OutputFormat<K, V> {

    private final String[] updateKeys;
    private final boolean multiUpdate;

    public void checkOutputSpecs(final JobContext context) throws IOException {
        List<MongoURI> outputUris;
        outputUris = MongoConfigUtil.getOutputURIs(context.getConfiguration());
        if (outputUris == null || outputUris.size() == 0) {
            throw new IOException("No output URI is specified. You must set mongo.output.uri.");
        }
    }

    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) {
        return new MongoOutputCommitter();
    }

    /**
     * Get the record writer that points to the output collection.
     */
    public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext context) {
        return new MongoRecordWriter(MongoConfigUtil.getOutputCollections(context.getConfiguration()), context);
    }

    public MongoOutputFormat() {
        multiUpdate = false;
        updateKeys = null;
    }

    public MongoOutputFormat(final String[] updateKeys, final boolean multiUpdate) {
        this.updateKeys = updateKeys;
        this.multiUpdate = multiUpdate;
    }

    private static final Log LOG = LogFactory.getLog(MongoOutputFormat.class);
}

