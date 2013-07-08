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
import com.mongodb.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import java.io.IOException;

// Commons
// Hadoop

public class BSONFileOutputFormat<K, V> extends org.apache.hadoop.mapred.FileOutputFormat<K, V> {

    public void checkOutputSpecs( FileSystem fs, final JobConf job ){ }

    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        Path outPath;
        if(job.get("mapred.output.file") != null){
            outPath = new Path(job.get("mapred.output.file"));
        }else{
            outPath = getPathForCustomFile(job, name);
        }
        FileSystem fs = outPath.getFileSystem(job);
        FSDataOutputStream outFile = fs.create(outPath);

        FSDataOutputStream splitFile = null;
        if(MongoConfigUtil.getBSONOutputBuildSplits(job)){
            Path splitPath = new Path(outPath.getParent(),  "." + outPath.getName() + ".splits");
            splitFile = fs.create(splitPath);
        }

        long splitSize = BSONSplitter.getSplitSize(job, null);

        BSONFileRecordWriter<K,V> recWriter = new BSONFileRecordWriter(outFile, splitFile, splitSize);
        return recWriter;
    }

    private static final Log LOG = LogFactory.getLog( BSONFileOutputFormat.class );
}

