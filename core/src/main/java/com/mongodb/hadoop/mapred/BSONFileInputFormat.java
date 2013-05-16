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
    public org.apache.hadoop.mapred.FileSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        BSONSplitter splitter = new BSONSplitter();
        splitter.setConf(job);
        Path[] inputPaths = splitter.getInputPaths();
        ArrayList<FileStatus> filesToProcess = new ArrayList<FileStatus>();
        for(Path inputPath : inputPaths){
            filesToProcess.addAll(splitter.getFilesInPath(inputPath));
        }
        for(FileStatus inputFile : filesToProcess){
            Path path = inputFile.getPath();
            Path splitFilePath =  new Path(path.getParent(),  "." + path.getName() + ".splits");
            FileSystem fs = path.getFileSystem(job);
            try{
                splitter.loadSplitsFromSplitFile(inputFile, splitFilePath);
            }catch(BSONSplitter.NoSplitFileException nsfe){
                splitter.readSplitsForFile(inputFile);
            }
        }

        ArrayList<org.apache.hadoop.mapreduce.lib.input.FileSplit> newsplits = splitter.getAllSplits();
        ArrayList<org.apache.hadoop.mapred.FileSplit> results = new ArrayList<org.apache.hadoop.mapred.FileSplit>(newsplits.size());
        for(org.apache.hadoop.mapreduce.lib.input.FileSplit split : newsplits ){
            org.apache.hadoop.mapred.FileSplit fsplit = new org.apache.hadoop.mapred.FileSplit(split.getPath(), split.getStart(), split.getLength(), split.getLocations());
            results.add(fsplit);
        }
        return results.toArray(new org.apache.hadoop.mapred.FileSplit[0]);
    }

    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        BSONFileRecordReader reader = new BSONFileRecordReader();
        reader.initialize(split, job);
        return reader;
    }
}
