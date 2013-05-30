package com.mongodb.hadoop.mapred;

import com.mongodb.hadoop.util.BSONSplitter;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.mapred.input.BSONFileRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

import java.util.ArrayList;
import java.io.IOException;
import org.apache.commons.logging.*;

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

    private static final Log log = LogFactory.getLog( BSONFileInputFormat.class );

    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    @Override
    public org.apache.hadoop.mapred.FileSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        FileStatus[] inputFiles = listStatus(job);
        //Using fully-qualified class names here to avoid confusion between the two APIs, because
        //mapred.* vs. mapreduce.* are both needed here and it gets pretty impossible to read otherwise.
        ArrayList<org.apache.hadoop.mapred.FileSplit> results = new ArrayList<org.apache.hadoop.mapred.FileSplit>();
        for(FileStatus file : inputFiles){
            BSONSplitter splitter = new BSONSplitter();
            splitter.setConf(job);
            splitter.setInputPath(file.getPath());
            Path splitFilePath;
            splitFilePath = new Path(file.getPath().getParent(),  "." + file.getPath().getName() + ".splits");
            try{
                splitter.loadSplitsFromSplitFile(file, splitFilePath);
            }catch(BSONSplitter.NoSplitFileException nsfe){
                log.info("No split file for " + file + "; building split file");
                splitter.readSplitsForFile(file);
            }
            log.info("BSONSplitter found " + splitter.getAllSplits().size() + " splits.");

            for(org.apache.hadoop.mapreduce.lib.input.FileSplit split : splitter.getAllSplits() ){
                org.apache.hadoop.mapred.FileSplit fsplit =
                    new org.apache.hadoop.mapred.FileSplit(split.getPath(),
                                                           split.getStart(),
                                                           split.getLength(),
                                                           split.getLocations());
                results.add(fsplit);
            }
        }
        log.info("Total of " + results.size() + " found.");
        return results.toArray(new org.apache.hadoop.mapred.FileSplit[0]);
    }

    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        BSONFileRecordReader reader = new BSONFileRecordReader();
        reader.initialize(split, job);
        return reader;
    }
}
