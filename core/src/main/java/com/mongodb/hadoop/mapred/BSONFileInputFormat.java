package com.mongodb.hadoop.mapred;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.mapred.input.BSONFileRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;


import java.util.List;
import java.io.IOException;

/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class BSONFileInputFormat extends FileInputFormat {

    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) {
        final MongoConfig conf = new MongoConfig(job);
        // TODO - Support allowing specification of numSplits to affect our ops?
        final List<org.apache.hadoop.mapreduce.InputSplit> splits = MongoSplitter.calculateSplits( conf );
        return splits.toArray(new InputSplit[0]);
    }

    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        BSONFileRecordReader reader = new BSONFileRecordReader();
        reader.initialize(split, job);
        return reader;
    }
}
