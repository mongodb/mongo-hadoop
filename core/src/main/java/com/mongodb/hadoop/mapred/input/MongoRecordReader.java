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

package com.mongodb.hadoop.mapred.input;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.bson.BasicBSONObject;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class MongoRecordReader implements RecordReader<BSONWritable, BSONWritable> {

    private static final Log LOG = LogFactory.getLog(MongoRecordReader.class);
    
    private final DBCursor cursor;
    private BSONWritable currentVal = new BSONWritable();
    private BSONWritable currentKey = new BSONWritable();
    private float seen = 0;
    private float total;
    private String keyField;

    private MongoInputSplit split;

    public MongoRecordReader(final MongoInputSplit split) {
        this.split = split;
        cursor = split.getCursor();
        keyField = split.getKeyField();
    }

    public void close() {
        if (cursor != null) {
            cursor.close();
        }
    }


    public BSONWritable createKey() {
        return new BSONWritable();
    }

    public BSONWritable createValue() {
        return new BSONWritable();
    }

    public BSONWritable getCurrentKey() {
        return this.currentKey;
    }

    public BSONWritable getCurrentValue() {
        return this.currentVal;
    }

    public float getProgress() {
        try {
            if (cursor.hasNext()) {
                return 0.0f;
            } else {
                return 1.0f;
            }
        } catch (MongoException e) {
            return 1.0f;
        }
    }

    public long getPos() {
        return 0; // no progress to be reported, just working on it
    }

    public void initialize(final InputSplit split, final TaskAttemptContext context) {
        total = 1.0f;
    }

    public boolean nextKeyValue() throws IOException {
        try {
            if (!cursor.hasNext()) {
                LOG.info("Read " + seen + " documents from:");
                LOG.info(split.toString());
                return false;
            }

            DBObject next = cursor.next();
            this.currentVal.setDoc(next);
            this.currentKey.setDoc(new BasicBSONObject("_id", next.get("_id")));
            seen++;

            return true;
        } catch (MongoException e) {
            throw new IOException("Couldn't get next key/value from mongodb: ", e);
        }
    }

    public boolean next(final BSONWritable key, final BSONWritable value) throws IOException {
        if (nextKeyValue()) {
            key.setDoc(this.currentKey.getDoc());
            value.setDoc(this.currentVal.getDoc());
            return true;
        } else {
            LOG.info("Cursor exhausted.");
            return false;
        }
    }
}
