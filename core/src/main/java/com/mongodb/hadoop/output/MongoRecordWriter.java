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
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoOutput;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.io.MongoWritableTypes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MongoRecordWriter<K, V> extends RecordWriter<K, V> {

    private static final Log LOG = LogFactory.getLog(MongoRecordWriter.class);
    private final List<DBCollection> collections;
    private final TaskAttemptContext context;
    private final BSONWritable bsonWritable;
    private FSDataOutputStream outputStream;

    /**
     * Create a MongoRecordWriter targeting a single DBCollection.
     * @param c a DBCollection
     * @param ctx the TaskAttemptContext
     */
    public MongoRecordWriter(final DBCollection c, final TaskAttemptContext ctx) {
        this(Arrays.asList(c), ctx);
    }

    /**
     * Create a MongoRecordWriter that targets multiple DBCollections.
     * @param c a list of DBCollections
     * @param ctx the TaskAttemptContext
     */
    public MongoRecordWriter(final List<DBCollection> c, final TaskAttemptContext ctx) {
        collections = new ArrayList<DBCollection>(c);
        context = ctx;
        bsonWritable = new BSONWritable();

        // Initialize output stream.
        try {
            FileSystem fs = FileSystem.get(ctx.getConfiguration());
            Path outputPath = MongoOutputCommitter.getTaskAttemptPath(ctx);
            LOG.info("Writing to temporary file: " + outputPath.toString());
            outputStream = fs.create(outputPath, true);
        } catch (IOException e) {
            LOG.error(
              "Could not open temporary file for buffering Mongo output", e);
        }
    }

    @Override
    public void close(final TaskAttemptContext context) {
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                LOG.error("Could not close output stream", e);
            }
        }
    }

    @Override
    public void write(final K key, final V value) throws IOException {
        if (value instanceof MongoUpdateWritable) {
            outputStream.writeInt(MongoWritableTypes.MONGO_UPDATE_WRITABLE);
            ((MongoUpdateWritable) value).write(outputStream);
        } else {
            DBObject o = new BasicDBObject();
            if (key instanceof BSONWritable) {
                o.put("_id", ((BSONWritable) key).getDoc());
            } else if (key instanceof BSONObject) {
                o.put("_id", key);
            } else {
                o.put("_id", BSONWritable.toBSON(key));
            }

            if (value instanceof BSONWritable) {
                o.putAll(((BSONWritable) value).getDoc());
            } else if (value instanceof MongoOutput) {
                ((MongoOutput) value).appendAsValue(o);
            } else if (value instanceof BSONObject) {
                o.putAll((BSONObject) value);
            } else {
                o.put("value", BSONWritable.toBSON(value));
            }
            outputStream.writeInt(MongoWritableTypes.BSON_WRITABLE);
            bsonWritable.setDoc(o);
            bsonWritable.write(outputStream);
        }
    }


    /**
     * Add an index to be ensured before the Job starts running.
     * @param index a DBObject describing the keys of the index.
     * @param options a DBObject describing the options to apply when creating
     *                the index.
     */
    public void ensureIndex(final DBObject index, final DBObject options) {
        // just do it on one mongod
        collections.get(0).createIndex(index, options);
    }

    /**
     * Get the TaskAttemptContext associated with this MongoRecordWriter.
     * @return the TaskAttemptContext
     */
    public TaskAttemptContext getContext() {
        return context;
    }
}
