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

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.input.BSONFileRecordReader;
import com.mongodb.hadoop.splitter.BSONSplitter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.*;

public class BSONFileInputFormat extends FileInputFormat {

    protected boolean isSplitable(final JobContext context, final Path filename) {
        return true;
    }

    @Override
    public FileSplit[] getSplits(final JobConf job, final int numSplits) throws IOException {

        FileStatus[] inputFiles = listStatus(job);
        List<FileSplit> results = new ArrayList<FileSplit>();
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
            LOG.info(format("BSONSplitter found %d splits.", splitter.getAllSplits().size()));

            for (org.apache.hadoop.mapreduce.lib.input.FileSplit split : splitter.getAllSplits()) {
                FileSplit fsplit =
                    new FileSplit(split.getPath(),
                                  split.getStart(),
                                  split.getLength(),
                                  split.getLocations());
                results.add(fsplit);
            }
        }
        LOG.info(format("Total of %d found.", results.size()));
        return results.toArray(new FileSplit[results.size()]);
    }

    @Override
    public RecordReader<NullWritable, BSONWritable> getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter) throws IOException {
        BSONFileRecordReader reader = new BSONFileRecordReader();
        reader.initialize(split, job);
        return reader;
    }
}
