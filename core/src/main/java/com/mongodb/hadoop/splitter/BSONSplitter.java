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

package com.mongodb.hadoop.splitter;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.input.BSONFileSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.LazyBSONCallback;
import org.bson.LazyBSONDecoder;
import org.bson.LazyBSONObject;

import java.io.IOException;
import java.util.ArrayList;

public class BSONSplitter extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(BSONSplitter.class);

    private ArrayList<BSONFileSplit> splitsList;
    private Path inputPath;
    private final BasicBSONCallback callback = new BasicBSONCallback();
    private final LazyBSONCallback lazyCallback = new LazyBSONCallback();
    private final LazyBSONDecoder lazyDec = new LazyBSONDecoder();
    private final BasicBSONDecoder bsonDec = new BasicBSONDecoder();
    private final BasicBSONEncoder bsonEnc = new BasicBSONEncoder();

    public static class NoSplitFileException extends Exception {
    }

    public void setInputPath(final Path p) {
        inputPath = p;
    }

    public ArrayList<BSONFileSplit> getAllSplits() {
        if (splitsList == null) {
            return new ArrayList<BSONFileSplit>(0);
        } else {
            return splitsList;
        }
    }

    public BSONFileSplit createFileSplitFromBSON(final BSONObject obj, final
    FileSystem fs, final FileStatus inputFile) throws IOException {
        long start = (Long) obj.get("s");
        long splitLen = (Long) obj.get("l");
        BSONFileSplit split;
        try {
            BlockLocation[] blkLocations = fs.getFileBlockLocations(inputFile, start, splitLen);
            int blockIndex = getLargestBlockIndex(blkLocations);
            split = new BSONFileSplit(inputFile.getPath(), start, splitLen,
                                      blkLocations[blockIndex].getHosts());
        } catch (IOException e) {
            LOG.warn("Couldn't find block locations when constructing input split from BSON. Using non-block-aware input split; "
                     + e.getMessage());
            split = new BSONFileSplit(inputFile.getPath(), start, splitLen,
                                      null);
        }
        split.setKeyField(MongoConfigUtil.getInputKey(getConf()));
        return split;
    }

    public BSONFileSplit createFileSplit(final FileStatus inFile, final
    FileSystem fs, final long splitStart, final long splitLen) {
        BSONFileSplit split;
        try {
            BlockLocation[] blkLocations = fs.getFileBlockLocations(inFile, splitStart, splitLen);
            int blockIndex = getLargestBlockIndex(blkLocations);
            split = new BSONFileSplit(inFile.getPath(), splitStart, splitLen,
                                      blkLocations[blockIndex].getHosts());
        } catch (IOException e) {
            LOG.warn("Couldn't find block locations when constructing input split from byte offset. Using non-block-aware input split; "
                     + e.getMessage());
            split = new BSONFileSplit(inFile.getPath(), splitStart, splitLen,
                                      null);
        }
        split.setKeyField(MongoConfigUtil.getInputKey(getConf()));
        return split;
    }

    public void loadSplitsFromSplitFile(final FileStatus inputFile, final Path splitFile) throws NoSplitFileException, IOException {
        ArrayList<BSONFileSplit> splits = new ArrayList<BSONFileSplit>();
        FileSystem fs = splitFile.getFileSystem(getConf()); // throws IOException
        FileStatus splitFileStatus;
        FSDataInputStream fsDataStream;
        try {
            try {
                splitFileStatus = fs.getFileStatus(splitFile);
                LOG.info("Found split file at : " + splitFileStatus);
            } catch (Exception e) {
                throw new NoSplitFileException();
            }
            fsDataStream = fs.open(splitFile); // throws IOException
            while (fsDataStream.getPos() < splitFileStatus.getLen()) {
                callback.reset();
                bsonDec.decode(fsDataStream, callback);
                BSONObject splitInfo = (BSONObject) callback.get();
                splits.add(createFileSplitFromBSON(splitInfo, fs, inputFile));
            }
        } finally {
            fs.close();
        }
        fsDataStream.close();
        splitsList = splits;
    }

    public static long getSplitSize(final Configuration conf, final FileStatus file) {
        long minSize = Math.max(1L, conf.getLong("mapred.min.split.size", 1L));
        long maxSize = conf.getLong("mapred.max.split.size", Long.MAX_VALUE);

        if (file != null) {
            long fileBlockSize = file.getBlockSize();
            return Math.max(minSize, Math.min(maxSize, fileBlockSize));
        } else {
            long blockSize = conf.getLong("dfs.blockSize", 64 * 1024 * 1024);
            return Math.max(minSize, Math.min(maxSize, blockSize));
        }
    }

    public void readSplitsForFile(final FileStatus file) throws IOException {
        Path path = file.getPath();
        ArrayList<BSONFileSplit> splits = new ArrayList<BSONFileSplit>();
        FileSystem fs = path.getFileSystem(getConf());
        long length = file.getLen();
        if (!getConf().getBoolean("bson.split.read_splits", true)) {
            LOG.info("Reading splits is disabled - constructing single split for " + file);
            BSONFileSplit onesplit = createFileSplit(file, fs, 0, length);
            splits.add(onesplit);
            splitsList = splits;
            return;
        }
        if (length != 0) {
            int numDocsRead = 0;
            long splitSize = getSplitSize(getConf(), file);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Generating splits for " + path + " of up to " + splitSize + " bytes.");
            }
            FSDataInputStream fsDataStream = fs.open(path);
            long curSplitLen = 0;
            long curSplitStart = 0;
            try {
                while (fsDataStream.getPos() + 1 < length) {
                    lazyCallback.reset();
                    lazyDec.decode(fsDataStream, lazyCallback);
                    LazyBSONObject bo = (LazyBSONObject) lazyCallback.get();
                    int bsonDocSize = bo.getBSONSize();
                    if (curSplitLen + bsonDocSize >= splitSize) {
                        BSONFileSplit split = createFileSplit(file, fs,
                                                              curSplitStart, curSplitLen);
                        splits.add(split);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("Creating new split (%d) %s", splits.size(), split));
                        }
                        curSplitStart = fsDataStream.getPos() - bsonDocSize;
                        curSplitLen = 0;
                    }
                    curSplitLen += bsonDocSize;
                    numDocsRead++;
                    if (numDocsRead % 1000 == 0) {
                        float splitProgress = 100f * ((float) fsDataStream.getPos() / length);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("Read %d docs calculating splits for %s; %3.3f%% complete.",
                                                    numDocsRead, file.getPath(), splitProgress));
                        }
                    }
                }
                if (curSplitLen > 0) {
                    BSONFileSplit split = createFileSplit(file, fs,
                                                          curSplitStart, curSplitLen);
                    splits.add(split);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Final split (%d) %s", splits.size(), split.getPath()));
                    }
                }
                splitsList = splits;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Completed splits calculation for " + file.getPath());
                }
                writeSplits();
            } catch (IOException e) {
                LOG.warn("IOException: " + e);
            } finally {
                fsDataStream.close();
            }
        } else {
            LOG.warn("Zero-length file, skipping split calculation.");
        }
    }

    public void writeSplits() throws IOException {
        if (getConf().getBoolean("bson.split.write_splits", true)) {
            LOG.info("Writing splits to disk.");
        } else {
            LOG.info("bson.split.write_splits is set to false - skipping writing splits to disk.");
            return;
        }

        if (splitsList == null) {
            LOG.info("No splits found, skipping write of splits file.");
        }

        Path outputPath = getSplitsFilePath(inputPath, getConf());
        FileSystem pathFileSystem = outputPath.getFileSystem(getConf());
        FSDataOutputStream fsDataOut = null;
        try {
            fsDataOut = pathFileSystem.create(outputPath, false);
            for (FileSplit inputSplit : splitsList) {
                BSONObject splitObj = BasicDBObjectBuilder.start()
                                                          .add("s", inputSplit.getStart())
                                                          .add("l", inputSplit.getLength()).get();
                byte[] encodedObj = bsonEnc.encode(splitObj);
                fsDataOut.write(encodedObj, 0, encodedObj.length);
            }
        } catch (IOException e) {
            LOG.error("Could not create splits file: " + e.getMessage());
            throw e;
        } finally {
            if (fsDataOut != null) {
                fsDataOut.close();
            }
        }
    }

    public void readSplits() throws IOException {
        splitsList = new ArrayList<BSONFileSplit>();
        if (inputPath == null) {
            throw new IllegalStateException("Input path has not been set.");
        }
        FileSystem fs = inputPath.getFileSystem(getConf());
        FileStatus file = fs.getFileStatus(inputPath);
        readSplitsForFile(file);
    }

    @Override
    public int run(final String[] args) throws Exception {
        setInputPath(new Path(getConf().get("mapred.input.dir", "")));
        readSplits();
        writeSplits();
        return 0;
    }

    public static int getLargestBlockIndex(final BlockLocation[] blockLocations) {
        int retVal = -1;
        if (blockLocations == null) {
            return retVal;
        }
        long max = 0;
        for (int i = 0; i < blockLocations.length; i++) {
            BlockLocation blk = blockLocations[i];
            if (blk.getLength() > max) {
                retVal = i;
            }
        }
        return retVal;
    }

    /**
     * Get the path to the ".splits" file for a BSON file.
     * @param filePath the path to the BSON file.
     * @param conf the Hadoop configuration.
     * @return the path to the ".splits" file.
     */
    public static Path getSplitsFilePath(final Path filePath, final Configuration conf) {
        String splitsPath = MongoConfigUtil.getBSONSplitsPath(conf);
        String splitsFileName = "." + filePath.getName() + ".splits";
        if (null == splitsPath) {
            return new Path(filePath.getParent(), splitsFileName);
        }
        return new Path(splitsPath, splitsFileName);
    }

    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new BSONSplitter(), args));
    }

}
