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

package com.mongodb.hadoop.mapred;

import com.mongodb.hadoop.mapred.input.BSONFileRecordReader;
import com.mongodb.hadoop.splitter.BSONSplitter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;

//import org.apache.hadoop.mapred.FileSplit;

public class BSONFileInputFormat extends FileInputFormat {

    protected boolean isSplitable(final JobContext context, final Path filename) {
        return true;
    }

    @Override
    public org.apache.hadoop.mapred.FileSplit[] getSplits(final JobConf job, final int numSplits) throws IOException {

        FileStatus[] inputFiles = listStatus(job);
        //Using fully-qualified class names here to avoid confusion between the two APIs, because
        //mapred.* vs. mapreduce.* are both needed here and it gets pretty impossible to read otherwise.
        ArrayList<org.apache.hadoop.mapred.FileSplit> results = new ArrayList<org.apache.hadoop.mapred.FileSplit>();
        for (FileStatus file : inputFiles) {
            BSONSplitter splitter = new BSONSplitter();
            splitter.setConf(job);
            splitter.setInputPath(file.getPath());
            Path splitFilePath;
            splitFilePath = new Path(file.getPath().getParent(), "." + file.getPath().getName() + ".splits");
            try {
                splitter.loadSplitsFromSplitFile(file, splitFilePath);
            } catch (BSONSplitter.NoSplitFileException nsfe) {
                LOG.info("No split file for " + file + "; building split file");
                splitter.readSplitsForFile(file);
            }
            LOG.info("BSONSplitter found " + splitter.getAllSplits().size() + " splits.");

            for (org.apache.hadoop.mapreduce.lib.input.FileSplit split : splitter.getAllSplits()) {
                org.apache.hadoop.mapred.FileSplit fsplit =
                    new org.apache.hadoop.mapred.FileSplit(split.getPath(),
                                                           split.getStart(),
                                                           split.getLength(),
                                                           split.getLocations());
                results.add(fsplit);
            }
        }
        LOG.info("Total of " + results.size() + " found.");
        return results.toArray(new org.apache.hadoop.mapred.FileSplit[results.size()]);
    }

    @Override
    public RecordReader getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter) throws IOException {
        BSONFileRecordReader reader = new BSONFileRecordReader();
        reader.initialize(split, job);
        return reader;
    }
}
