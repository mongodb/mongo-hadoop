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

import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class BSONFileInputFormat extends FileInputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        BSONFileRecordReader reader = new BSONFileRecordReader();
        reader.initialize(split, context);
		System.out.println(reader);
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
		for (FileStatus file : statuses) {
			Path path = file.getPath();
			System.out.println("generating splits for " + path);
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			if (length != 0) { 
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);

				long bytesRemaining = length;

				ByteBuffer bb;
				byte[] headerBuf = new byte[4];
				int numDocs = 0;
				FSDataInputStream fsDataStream = fs.open(path);
				long curSplitLen = 0;
				long curSplitStart = 0;
				while(fsDataStream.getPos() + 1 < length){
					fsDataStream.read(fsDataStream.getPos(), headerBuf, 0, 4);
					//TODO check that 4 bytes were actually read successfully, otherwise error
					//TODO just parse the integer so we don't init a bytebuf every time
					bb = ByteBuffer.wrap(headerBuf);
					bb.order(ByteOrder.LITTLE_ENDIAN);
					int bsonDocSize = bb.getInt();
					if(curSplitLen + bsonDocSize >= splitSize){
						splits.add(new FileSplit(path, curSplitStart,
								   curSplitLen,
								   blkLocations[blkLocations.length-1].getHosts()));
						curSplitLen = 0;
						curSplitStart = fsDataStream.getPos() + bsonDocSize;
						System.out.println("new split " + splits.size());
					}
					curSplitLen += bsonDocSize;

					fsDataStream.seek(fsDataStream.getPos() + bsonDocSize);
					numDocs++;
					if(numDocs % 10000 == 0){
						System.out.println("got " + numDocs + " docs");
					}
				}
				if(curSplitLen > 0){
					System.out.println("one more split");
					splits.add(new FileSplit(path, curSplitStart,
							   curSplitLen,
							   blkLocations[blkLocations.length-1].getHosts()));
				}

			}
		}

		return splits;

    }

}
