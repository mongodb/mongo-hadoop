/*
 * Copyright 2010 10gen Inc.
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

// Mongo

import com.mongodb.hadoop.mapred.output.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

// Commons
// Hadoop

public class BSONFileOutputFormat<K, V> implements OutputFormat<K, V> {

    public void checkOutputSpecs( FileSystem fs, final JobConf job ){ }

    public RecordWriter<K, V> getRecordWriter(FileSystem fs, JobConf job, String name, Progressable progress){
        return new BSONFileRecordWriter(job);
    }

    private static final Log LOG = LogFactory.getLog( BSONFileOutputFormat.class );
}

