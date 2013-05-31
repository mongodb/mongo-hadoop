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

package com.mongodb.hadoop.mapred.output;

import com.mongodb.*;
import com.mongodb.hadoop.mapred.*;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordWriter;
import org.bson.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.*;


public class BSONFileRecordWriter<K, V> extends com.mongodb.hadoop.output.BSONFileRecordWriter implements RecordWriter<K, V> {

    public BSONFileRecordWriter(Configuration conf){
        super(conf);
    }

    public void close( Reporter reporter ) throws IOException{
        this.close((TaskAttemptContext)null);
    }

}

