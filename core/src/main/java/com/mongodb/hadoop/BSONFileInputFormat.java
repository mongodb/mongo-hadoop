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

    @Override
    public List<InputSplit> getSplits( JobContext context ) throws IOException{
        final Configuration hadoopConfiguration = context.getConfiguration();
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(context));
		long maxSize = getMaxSplitSize(context);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		Path[] bsonInputPaths = getInputPaths(context);
		List<FileStatus> statuses = listStatus(context);
        log.info("files to process: ");
        for(FileStatus s : statuses){
            log.info(s.getPath().toString());
        }

		for (FileStatus file : statuses) {
			Path path = file.getPath();
            Path splitFilePath =  new Path(path.getParent(),  
                                           "." + path.getName()
                                           + ".splits");
            FileSystem fs = path.getFileSystem(hadoopConfiguration);
            FileStatus splitFileStatus = null;
            try{
                splitFileStatus = fs.getFileStatus(splitFilePath);
            }catch(IOException ioe){
                log.info("no split file found.");
                //split file not found
            }
            if(splitFileStatus == null || splitFileStatus.isDir()){
                log.error("no pre-built splits found for " + path + " at " + splitFilePath);
                BSONSplitter bsonSplitter = new BSONSplitter();
                bsonSplitter.setConf(hadoopConfiguration);
                bsonSplitter.setInputPath(path);
                bsonSplitter.readSplits();
                try{
                    bsonSplitter.writeSplits();
                }catch(IOException ioe){
                    log.error("couldn't save splits information: " + ioe.getMessage());
                }

                Map<Path, List<FileSplit>> splitsMap = bsonSplitter.getSplitsMap();
                for(Map.Entry<Path, List<FileSplit>> entry : splitsMap.entrySet()) {
                    Path key = entry.getKey();
                    List<FileSplit> value = entry.getValue();
                    splits.addAll(value);
                }
            }else{
                log.info("Found splits file at: " + splitFilePath);
                BSONSplitter bsonSplitter = new BSONSplitter();
                bsonSplitter.setConf(hadoopConfiguration);
                bsonSplitter.loadSplits(splitFileStatus, file);
                Map<Path, List<FileSplit>> splitsMap = bsonSplitter.getSplitsMap();
                for(Map.Entry<Path, List<FileSplit>> entry : splitsMap.entrySet()) {
                    Path key = entry.getKey();
                    List<FileSplit> value = entry.getValue();
                    splits.addAll(value);
                }
                
            }
        }

        for(InputSplit s : splits){
            log.info("split at: " + s.toString());
        }

        return splits;
		//return new ArrayList<InputSplit>();

    }

}
