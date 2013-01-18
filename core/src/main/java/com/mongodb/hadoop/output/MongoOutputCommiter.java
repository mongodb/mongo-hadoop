/*
 * Copyright 2011 10gen Inc.
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

package com.mongodb.hadoop.output;

import org.apache.hadoop.mapreduce.*;
import org.apache.commons.logging.*;

public class MongoOutputCommiter extends OutputCommitter {

    private static final Log log = LogFactory.getLog( MongoOutputCommiter.class );

    public void abortTask( TaskAttemptContext taskContext ){
        log.info("Aborting task.");
    }

    public void cleanupJob( JobContext jobContext ){
        log.info("Cleaning up job.");
    }

    public void commitTask( TaskAttemptContext taskContext ){
        log.info("Committing task.");
    }

    public boolean needsTaskCommit( TaskAttemptContext taskContext ){
        return true;
    }

    public void setupJob( JobContext jobContext ){
        log.info("Setting up job.");
    }

    public void setupTask( TaskAttemptContext taskContext ){
        log.info("Setting up task.");
    }

}
