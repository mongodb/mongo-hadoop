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
import com.mongodb.hadoop.mapred.input.BSONFileSplit;
import com.mongodb.hadoop.splitter.BSONSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.hadoop.splitter.BSONSplitter.getSplitsFilePath;
import static java.lang.String.format;

public class BSONFileInputFormat extends FileInputFormat {

    @Override
    protected boolean isSplitable(final FileSystem fs, final Path filename) {
        return true;
    }

    @Override
    public FileSplit[] getSplits(final JobConf job, final int numSplits) throws IOException {

        BSONSplitter splitter = new BSONSplitter();
        splitter.setConf(job);
        FileStatus[] inputFiles = listStatus(job);
        List<FileSplit> results = new ArrayList<FileSplit>();
        for (FileStatus file : inputFiles) {
            splitter.setInputPath(file.getPath());

            Path splitFilePath = getSplitsFilePath(file.getPath(), job);
            try {
                splitter.loadSplitsFromSplitFile(file, splitFilePath);
            } catch (BSONSplitter.NoSplitFileException nsfe) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(format("No split file for %s; building split file", file.getPath()));
                }
                splitter.readSplitsForFile(file);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(format("BSONSplitter found %d splits.", splitter.getAllSplits().size()));
            }

            for (org.apache.hadoop.mapreduce.lib.input.FileSplit split : splitter.getAllSplits()) {
                BSONFileSplit fsplit = new BSONFileSplit(
                  split.getPath(),
                  split.getStart(),
                  split.getLength(),
                  split.getLocations());
                fsplit.setKeyField(MongoConfigUtil.getInputKey(job));
                results.add(fsplit);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(format("Total of %d found.", results.size()));
        }
        return results.toArray(new BSONFileSplit[results.size()]);
    }


    @Override
    public RecordReader<NullWritable, BSONWritable> getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter)
        throws IOException {

        if (split instanceof BSONFileSplit) {
            BSONFileRecordReader reader = new BSONFileRecordReader();
            reader.initialize(split, job);
            return reader;
        }

        // Split was not created by BSONSplitter.
        FileSplit fileSplit = (FileSplit) split;
        BSONSplitter splitter = new BSONSplitter();
        splitter.setConf(job);
        splitter.setInputPath(fileSplit.getPath());
        org.apache.hadoop.mapreduce.lib.input.FileSplit newStyleFileSplit =
          new org.apache.hadoop.mapreduce.lib.input.FileSplit(
            fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(),
            fileSplit.getLocations());
        long start = splitter.getStartingPositionForSplit(newStyleFileSplit);

        BSONFileRecordReader reader = new BSONFileRecordReader(start);
        reader.initialize(fileSplit, job);
        return reader;
    }
}
