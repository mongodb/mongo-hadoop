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
package com.mongodb.hadoop;

import com.mongodb.hadoop.input.BSONFileRecordReader;
import com.mongodb.hadoop.input.BSONFileSplit;
import com.mongodb.hadoop.splitter.BSONSplitter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.hadoop.splitter.BSONSplitter.getSplitsFilePath;


public class BSONFileInputFormat extends FileInputFormat {

    private static final Log LOG = LogFactory.getLog(BSONFileInputFormat.class);

    /**
     * Determine if the given file is splittable. If the file is compressed,
     * it cannot be split.
     * @param context the current JobContext
     * @param filename the input file name
     * @return {@code true} if the file is splittable, {@code false} otherwise
     */
    @Override
    protected boolean isSplitable(
      final JobContext context, final Path filename) {
        CompressionCodec codec =
          new CompressionCodecFactory(
            context.getConfiguration()).getCodec(filename);
        // If BSON is compressed, it cannot be split.
        return null == codec;
    }

    @Override
    public RecordReader createRecordReader(final InputSplit split, final TaskAttemptContext context)
        throws IOException, InterruptedException {

        if (split instanceof BSONFileSplit) {
            // Split was created by BSONSplitter and starts at a whole document.
            return new BSONFileRecordReader();
        }

        // Split was not created by BSONSplitter, and we need to find the
        // first document to begin iterating.
        FileSplit fileSplit = (FileSplit) split;
        BSONSplitter splitter = new BSONSplitter();
        splitter.setConf(context.getConfiguration());
        splitter.setInputPath(fileSplit.getPath());

        return new BSONFileRecordReader(
          splitter.getStartingPositionForSplit(fileSplit));
    }

    public static PathFilter getInputPathFilter(final JobContext context) {
        Configuration conf = context.getConfiguration();
        Class<?> filterClass = conf.getClass("bson.pathfilter.class", null, PathFilter.class);
        return filterClass != null ? (PathFilter) ReflectionUtils.newInstance(filterClass, conf) : null;
    }

    @Override
    public List<FileSplit> getSplits(final JobContext context) throws IOException {
        Configuration config = context.getConfiguration();
        PathFilter pf = getInputPathFilter(context);
        BSONSplitter splitter = new BSONSplitter();
        splitter.setConf(config);
        ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
        List<FileStatus> inputFiles = listStatus(context);
        for (FileStatus file : inputFiles) {
            if (pf != null && !pf.accept(file.getPath())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("skipping file %s not matched path filter.", file.getPath()));
                }
                continue;
            } else if (!isSplitable(context, file.getPath())) {
                LOG.info(
                  "File " + file.getPath() + " is compressed so "
                    + "cannot be split.");
                splits.add(
                  splitter.createFileSplit(
                    file, FileSystem.get(file.getPath().toUri(), config),
                    0L, file.getLen()));
                continue;
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("processing file " + file.getPath());
            }

            splitter.setInputPath(file.getPath());

            Path splitFilePath = getSplitsFilePath(file.getPath(), config);
            try {
                splitter.loadSplitsFromSplitFile(file, splitFilePath);
            } catch (BSONSplitter.NoSplitFileException nsfe) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("No split file for %s; building split file", file.getPath()));
                }
                splitter.readSplitsForFile(file);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("BSONSplitter found %d splits.", splitter.getAllSplits().size()));
            }
            splits.addAll(splitter.getAllSplits());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Total of %d found.", splits.size()));
        }
        return splits;
    }

}
