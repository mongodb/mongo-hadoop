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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class MongoOutputCommitter extends OutputCommitter {
    private static final Log LOG = LogFactory.getLog(MongoOutputCommitter.class);

    public void abortTask(final TaskAttemptContext taskContext) {
        LOG.info("should abort task");
    }

    public void commitTask(final TaskAttemptContext taskContext) {
        LOG.info("should commit task");
    }

    public boolean needsTaskCommit(final TaskAttemptContext taskContext) {
        return true;
    }

    public void setupJob(final JobContext jobContext) {
        LOG.info("should setup job");
    }

    public void setupTask(final TaskAttemptContext taskContext) {
        LOG.info("should setup context");
    }
}
