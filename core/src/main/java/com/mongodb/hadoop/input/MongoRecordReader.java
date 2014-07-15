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

package com.mongodb.hadoop.input;

// Mongo

import com.mongodb.DBCursor;
import com.mongodb.MongoException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;

// Hadoop
// Commons

public class MongoRecordReader extends RecordReader<Object, BSONObject> {

    public MongoRecordReader(final MongoInputSplit split) {
        this.split = split;
        cursor = split.getCursor();
    }

    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
            cursor.getCollection().getDB().getMongo().close();
        }
    }

    @Override
    public Object getCurrentKey() {
        return current.get(split.getKeyField());
    }

    @Override
    public BSONObject getCurrentValue() {
        return current;
    }

    public float getProgress() {
        try {
            return cursor.hasNext() ? 0.0f : 1.0f;
        } catch (MongoException e) {
            return 1.0f;
        }
    }

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) {
        total = 1.0f;
    }

    @Override
    public boolean nextKeyValue() {
        try {
            if (!cursor.hasNext()) {
                LOG.info("Read " + seen + " documents from:");
                LOG.info(split.toString());
                return false;
            }

            current = cursor.next();
            seen++;

            return true;
        } catch (MongoException e) {
            LOG.error("Exception reading next key/val from mongo: " + e.getMessage());
            return false;
        }
    }


    private BSONObject current;
    private final MongoInputSplit split;
    private final DBCursor cursor;
    private float seen = 0;
    private float total;

    private static final Log LOG = LogFactory.getLog(MongoRecordReader.class);

}
