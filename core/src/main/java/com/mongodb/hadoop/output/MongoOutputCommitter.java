package com.mongodb.hadoop.output;

import org.apache.hadoop.mapreduce.*;
import org.apache.commons.logging.*;

public class MongoOutputCommitter extends OutputCommitter {

    private static final Log log = LogFactory.getLog( MongoOutputCommitter.class );

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

