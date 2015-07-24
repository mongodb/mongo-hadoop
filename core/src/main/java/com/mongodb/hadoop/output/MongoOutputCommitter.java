/*
 * Copyright 2011-2013 10gen Inc.
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

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.io.MongoWritableTypes;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class MongoOutputCommitter extends OutputCommitter {

    private static final Log LOG = LogFactory.getLog(MongoOutputCommitter.class);
    private int roundRobinCounter = 0;
    private final List<DBCollection> collections;
    private final int numberOfHosts;
    public static final String TEMP_DIR_NAME = "_MONGO_OUT_TEMP";

    public MongoOutputCommitter(final List<DBCollection> collections) {
        this.collections = collections;
        numberOfHosts = this.collections.size();
    }

    private void cleanupTemporaryFiles(final TaskAttemptContext taskContext)
      throws IOException {
        Path tempPath = getTaskAttemptPath(taskContext);
        try {
            FileSystem fs = FileSystem.get(taskContext.getConfiguration());
            fs.delete(tempPath, true);
        } catch (IOException e) {
            LOG.error("Could not delete temporary file " + tempPath, e);
            throw e;
        }
    }

    @Override
    public void abortTask(final TaskAttemptContext taskContext)
      throws IOException {
        LOG.info("Aborting task.");
        cleanupTemporaryFiles(taskContext);
    }

    @Override
    public void commitTask(final TaskAttemptContext taskContext)
      throws IOException {
        LOG.info("Committing task.");

        // Get temporary file.
        Path tempFilePath = getTaskAttemptPath(taskContext);
        LOG.info("Committing from temporary file: " + tempFilePath.toString());
        long filePos = 0, fileLen;
        FSDataInputStream inputStream = null;
        try {
            FileSystem fs = FileSystem.get(taskContext.getConfiguration());
            inputStream = fs.open(tempFilePath);
            fileLen = fs.getFileStatus(tempFilePath).getLen();
        } catch (IOException e) {
            LOG.error("Could not open temporary file for committing", e);
            cleanupAfterCommit(inputStream, taskContext);
            throw e;
        }

        int maxDocs = MongoConfigUtil.getBatchSize(
          taskContext.getConfiguration());
        int curBatchSize = 0;
        DBCollection coll = getDbCollectionByRoundRobin();
        BulkWriteOperation bulkOp = coll.initializeOrderedBulkOperation();

        // Read Writables out of the temporary file.
        BSONWritable bw = new BSONWritable();
        MongoUpdateWritable muw = new MongoUpdateWritable();
        while (filePos < fileLen) {
            try {
                // Determine writable type, and perform corresponding operation
                // on MongoDB.
                int mwType = inputStream.readInt();
                if (MongoWritableTypes.BSON_WRITABLE == mwType) {
                    bw.readFields(inputStream);
                    bulkOp.insert(new BasicDBObject(bw.getDoc().toMap()));
                } else if (MongoWritableTypes.MONGO_UPDATE_WRITABLE == mwType) {
                    muw.readFields(inputStream);
                    DBObject query = new BasicDBObject(muw.getQuery().toMap());
                    DBObject modifiers =
                      new BasicDBObject(muw.getModifiers().toMap());
                    if (muw.isMultiUpdate()) {
                        if (muw.isUpsert()) {
                            bulkOp.find(query).upsert().update(modifiers);
                        } else {
                            bulkOp.find(query).update(modifiers);
                        }
                    } else {
                        if (muw.isUpsert()) {
                            bulkOp.find(query).upsert().updateOne(modifiers);
                        } else {
                            bulkOp.find(query).updateOne(modifiers);
                        }
                    }
                } else {
                    throw new IOException("Unrecognized type: " + mwType);
                }
                filePos = inputStream.getPos();
                // Write to MongoDB if the batch is full, or if this is the last
                // operation to be performed for the Task.
                if (++curBatchSize >= maxDocs || filePos >= fileLen) {
                    try {
                        bulkOp.execute();
                    } catch (MongoException e) {
                        LOG.error("Could not write to MongoDB", e);
                        throw e;
                    }
                    coll = getDbCollectionByRoundRobin();
                    bulkOp = coll.initializeOrderedBulkOperation();
                    curBatchSize = 0;

                    // Signal progress back to Hadoop framework so that we
                    // don't time out.
                    taskContext.progress();
                }
            } catch (IOException e) {
                LOG.error("Error reading from temporary file", e);
                throw e;
            }
        }

        cleanupAfterCommit(inputStream, taskContext);
    }

    /**
     * Helper method to close an FSDataInputStream and clean up any files
     * still left around from map/reduce tasks.
     * @param inputStream the FSDataInputStream to close.
     */
    private void cleanupAfterCommit(
      final FSDataInputStream inputStream, final TaskAttemptContext context)
      throws IOException {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                LOG.error("Could not close input stream", e);
                throw e;
            }
        }
        cleanupTemporaryFiles(context);
    }

    @Override
    public boolean needsTaskCommit(final TaskAttemptContext taskContext)
      throws IOException {
        try {
            FileSystem fs = FileSystem.get(taskContext.getConfiguration());
            // Commit is only necessary if there was any output.
            return fs.exists(getTaskAttemptPath(taskContext));
        } catch (IOException e) {
            LOG.error("Could not open filesystem", e);
            throw e;
        }
    }

    @Override
    public void setupJob(final JobContext jobContext) {
        LOG.info("Setting up job.");
    }

    @Override
    public void setupTask(final TaskAttemptContext taskContext) {
        LOG.info("Setting up task.");
    }

    /**
     * Get the Path to where temporary files should be stored for a
     * TaskAttempt, whose TaskAttemptContext is provided.
     *
     * @param context the TaskAttemptContext.
     * @return the Path to the temporary file for the TaskAttempt.
     */
    public static Path getTaskAttemptPath(final TaskAttemptContext context) {
        Configuration config = context.getConfiguration();
        // Try to use the following base temporary directories, in this order:
        // 1. New-style option for task tmp dir
        // 2. Old-style option for task tmp dir
        // 3. Hadoop system-wide tmp dir
        // 4. /tmp
        String basePath = config.get(
          "mapreduce.task.tmp.dir",
          config.get(
            "mapred.child.tmp",
            config.get("hadoop.tmp.dir", "/tmp")));
        // Hadoop Paths always use "/" as a directory separator.
        return new Path(
          String.format("%s/%s/%s/_out",
            basePath, context.getTaskAttemptID().toString(), TEMP_DIR_NAME));
    }

    private DBCollection getDbCollectionByRoundRobin() {
        int hostIndex = (roundRobinCounter++ & 0x7FFFFFFF) % numberOfHosts;
        return collections.get(hostIndex);
    }

}

