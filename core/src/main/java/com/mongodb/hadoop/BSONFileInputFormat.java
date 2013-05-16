package com.mongodb.hadoop;
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

import com.mongodb.hadoop.input.BSONFileRecordReader;
import com.mongodb.hadoop.util.BSONSplitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import java.io.IOException;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.logging.*;


public class BSONFileInputFormat extends FileInputFormat {

    private static final Log log = LogFactory.getLog( BSONFileInputFormat.class );

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        BSONFileRecordReader reader = new BSONFileRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    public List<FileSplit> getSplits(JobContext context) throws IOException{
        log.info(context.getConfiguration());
        Configuration config = context.getConfiguration();
        BSONSplitter splitter = new BSONSplitter();
        splitter.setConf(config);
        Path[] inputPaths = splitter.getInputPaths();
        List<FileStatus> filesToProcess = new ArrayList<FileStatus>();
        for(Path inputPath : inputPaths){
            filesToProcess.addAll(splitter.getFilesInPath(inputPath));
        }
        for(FileStatus inputFile : filesToProcess){
        Path path = inputFile.getPath();
            Path splitFilePath =  new Path(path.getParent(),  "." + path.getName() + ".splits");
            FileSystem fs = path.getFileSystem(config);
            try{
                splitter.loadSplitsFromSplitFile(inputFile, splitFilePath);
            }catch(BSONSplitter.NoSplitFileException nsfe){
                splitter.readSplitsForFile(inputFile);
            }
        }
        log.info("BSONSplitter returned " + splitter.getAllSplits().size() + " splits.");
        return splitter.getAllSplits();
    }

}
