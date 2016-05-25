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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
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
import java.util.List;

public class BSONSplitter extends Configured implements Tool {
    private static final String CORE_JAR = "mongo-hadoop-core.jar";
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

    public BSONFileSplit createFileSplitFromBSON(
      final BSONObject obj, final FileSystem fs, final FileStatus inputFile)
      throws IOException {
        long start = (Long) obj.get("s");
        long splitLen = (Long) obj.get("l");
        return createFileSplit(inputFile, fs, start, splitLen);
    }

    public BSONFileSplit createFileSplit(final FileStatus inFile, final
    FileSystem fs, final long splitStart, final long splitLen) {
        BSONFileSplit split;
        try {
            BlockLocation[] blkLocations;

            // This code is based off of org.apache.hadoop.mapreduce.lib
            // .input.FileInputFormat.getSplits()

			blkLocations = fs.getFileBlockLocations(
                  inFile, splitStart, splitLen);


            int blockIndex = getBlockIndex(blkLocations, splitStart);
            split = new BSONFileSplit(
              inFile.getPath(), splitStart, splitLen,
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

    /**
     * Load splits from a splits file.
     *
     * @param inputFile the file whose splits are contained in the splits file.
     * @param splitFile the Path to the splits file.
     * @throws NoSplitFileException if the splits file is not found.
     * @throws IOException when an error occurs reading from the file.
     */
    public void loadSplitsFromSplitFile(final FileStatus inputFile, final Path splitFile) throws NoSplitFileException, IOException {
        ArrayList<BSONFileSplit> splits = new ArrayList<BSONFileSplit>();
        FileSystem fs = splitFile.getFileSystem(getConf()); // throws IOException
        FileStatus splitFileStatus;
        FSDataInputStream fsDataStream = null;
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
            if (null != fsDataStream) {
                fsDataStream.close();
            }
        }
        splitsList = splits;
    }

    public static long getSplitSize(final Configuration conf, final FileStatus file) {
        // Try new configuration options first, but fall back to old ones.
        long maxSize = conf.getLong(
          "mapreduce.input.fileinputformat.split.maxsize",
          conf.getLong("mapred.max.split.size", Long.MAX_VALUE));
        long minSize = Math.max(
          1L, conf.getLong(
            "mapreduce.input.fileinputformat.split.minsize",
            conf.getLong("mapred.min.split.size", 1L)));

        if (file != null) {
            long fileBlockSize = file.getBlockSize();
            return Math.max(minSize, Math.min(maxSize, fileBlockSize));
        } else {
            long blockSize = conf.getLong("dfs.blockSize", 64 * 1024 * 1024);
            return Math.max(minSize, Math.min(maxSize, blockSize));
        }
    }

    /**
     * Calculate the splits for a given input file, sensitive to options such
     * as {@link com.mongodb.hadoop.util.MongoConfigUtil#BSON_READ_SPLITS bson.split.read_splits}.
     * This method always re-calculates the splits and will try to write the
     * splits file.
     *
     * @param file the FileStatus for which to calculate splits.
     * @throws IOException when an error occurs reading from the FileSystem
     *
     * @see #readSplits
     */
    public void readSplitsForFile(final FileStatus file) throws IOException {
        long length = file.getLen();
        if (!MongoConfigUtil.getBSONReadSplits(getConf())) {
            LOG.info("Reading splits is disabled - constructing single split for " + file);
            FileSystem fs = file.getPath().getFileSystem(getConf());
            BSONFileSplit onesplit = createFileSplit(file, fs, 0, length);
            ArrayList<BSONFileSplit> splits = new ArrayList<BSONFileSplit>();
            splits.add(onesplit);
            splitsList = splits;
            return;
        }
        if (length != 0) {
            splitsList = (ArrayList<BSONFileSplit>) splitFile(file);
            writeSplits();
        } else {
            LOG.warn("Zero-length file, skipping split calculation.");
        }
    }

    /**
     * Calculate the splits for a given input file according to the settings
     * for split size only. This method does not respect options like
     * {@link com.mongodb.hadoop.util.MongoConfigUtil#BSON_READ_SPLITS bson.split.read_splits}.
     *
     * @param file the FileStatus for which to calculate splits.
     * @return a List of the calculated splits.
     *
     * @throws IOException when an error occurs reading from the FileSystem
     */
    protected List<BSONFileSplit> splitFile(final FileStatus file)
      throws IOException {
        Path path = file.getPath();
        ArrayList<BSONFileSplit> splits = new ArrayList<BSONFileSplit>();
        FileSystem fs = path.getFileSystem(getConf());
        long length = file.getLen();

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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Completed splits calculation for " + file.getPath());
            }
        } catch (IOException e) {
            LOG.warn("IOException: " + e);
        } finally {
            fsDataStream.close();
        }
        return splits;
    }

    /**
     * Write out the splits file, if doing so has been enabled. Splits must
     * already have been calculated previously by a call to {@link
     * #readSplitsForFile readSplitsForFile} or {@link #readSplits readSplits}.
     *
     * @see com.mongodb.hadoop.util.MongoConfigUtil#BSON_WRITE_SPLITS
     *
     * @throws IOException when an error occurs writing the file
     */
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

    /**
     * Calculate splits for each file in the input path, sensitive to options such
     * as {@link com.mongodb.hadoop.util.MongoConfigUtil#BSON_READ_SPLITS bson.split.read_splits}.
     * This method always re-calculates the splits and will try to write the
     * splits file.
     *
     * @see #readSplitsForFile
     *
     * @throws IOException when an error occurs reading from the file
     */
    public void readSplits() throws IOException {
        splitsList = new ArrayList<BSONFileSplit>();
        if (inputPath == null) {
            throw new IllegalStateException("Input path has not been set.");
        }
        FileSystem fs = inputPath.getFileSystem(getConf());
        FileStatus file = fs.getFileStatus(inputPath);
        readSplitsForFile(file);
    }

    /**
     * Get the index of the block within the given BlockLocations that
     * contains the given offset. Raises IllegalArgumentException if the
     * offset is outside the file.
     *
     * @param blockLocations BlockLocations to search.
     * @param offset the offset into the file.
     * @return the index of the BlockLocation containing the offset.
     */
    private static int getBlockIndex(
      final BlockLocation[] blockLocations, final long offset) {
        for (int i = 0; i < blockLocations.length; i++) {
            BlockLocation bl = blockLocations[i];
            if (bl.getOffset() <= offset
              && offset < bl.getOffset() + bl.getLength()) {
                return i;
            }
        }
        BlockLocation lastBlock = blockLocations[blockLocations.length - 1];
        long fileLength = lastBlock.getOffset() + lastBlock.getLength() - 1;
        throw new IllegalArgumentException(
          String.format("Offset %d is outside the file [0..%d].",
            offset, fileLength));
    }

    /**
     * Get the position at which the BSONFileRecordReader should begin
     * iterating the given split. This may not be at the beginning of the split
     * if the splits were not calculated by BSONSplitter.
     *
     * @param split the FileSplit for which to find the starting position.
     * @return the position of the first complete document within the split.
     * @throws IOException when an error occurs while reading a file
     */
    public synchronized long getStartingPositionForSplit(final FileSplit split)
      throws IOException {

        FileSystem fs = split.getPath().getFileSystem(getConf());
        FileStatus file = fs.getFileStatus(split.getPath());
        ArrayList<BSONFileSplit> splits;
        BSONFileSplit[] splitsArr;

        // Get splits calculated on document boundaries.
        if (MongoConfigUtil.getBSONReadSplits(getConf())) {
            // Use the splits file to load splits on document boundaries.
            try {
                // Try to use the existing splits file.
                loadSplitsFromSplitFile(
                  file, getSplitsFilePath(file.getPath(), getConf()));
            } catch (NoSplitFileException e) {
                // Create a splits file from scratch.
                readSplitsForFile(file);
            }
            splits = getAllSplits();
        } else {
            // Can't use a splits file, so create splits from scratch.
            splits = (ArrayList<BSONFileSplit>) splitFile(file);
        }
        splitsArr = new BSONFileSplit[splits.size()];
        splits.toArray(splitsArr);

        // Get the first pre-calculated split occurring before the start of
        // the given split.
        long previousStart = split.getStart();
        long startIterating = 0;
        for (BSONFileSplit bfs : splitsArr) {
            if (bfs.getStart() >= split.getStart()) {
                startIterating = previousStart;
                break;
            }
            previousStart = bfs.getStart();
        }

        // Beginning at 'startIterating', jump to the first document that begins
        // at or beyond the given split.
        FSDataInputStream fsDataStream = null;
        long pos = startIterating;
        try {
            fsDataStream = fs.open(split.getPath());
            fsDataStream.seek(pos);
            while (pos < split.getStart()) {
                callback.reset();
                bsonDec.decode(fsDataStream, callback);
                pos = fsDataStream.getPos();
            }
        } finally {
            if (null != fsDataStream) {
                fsDataStream.close();
            }
        }

        return pos;
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

    private void printUsage() {
        // CHECKSTYLE:OFF
        System.err.println(
          "USAGE: hadoop jar " + CORE_JAR + " "
            + getClass().getName()
            + " <fileName> [-c compressionCodec] [-o outputDirectory]\n\n"
            + "Make sure to use the full path, including scheme, for "
            + "input and output paths.");
        // CHECKSTYLE:ON
    }

    /**
     * When run as a Tool, BSONSplitter can be used to pre-split and compress
     * BSON files. This can be especially useful before uploading large BSON
     * files to HDFS to save time. The compressed splits are written to the
     * given output path or to the directory containing the input file, if
     * the output path is unspecified. A ".splits" file is not generated, since
     * each output file is expected to be its own split.
     *
     * @param args command-line arguments. Run with zero arguments to see usage.
     * @return exit status
     * @throws Exception
     */
    @Override
    public int run(final String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            return 1;
        }
        // Parse command-line arguments.
        Path filePath = new Path(args[0]);
        String compressorName = null, outputDirectoryStr = null;
        Path outputDirectory;
        CompressionCodec codec;
        Compressor compressor;

        for (int i = 1; i < args.length; ++i) {
            if ("-c".equals(args[i]) && args.length > i) {
                compressorName = args[++i];
            } else if ("-o".equals(args[i]) && args.length > i) {
                outputDirectoryStr = args[++i];
            } else {
                // CHECKSTYLE:OFF
                System.err.println("unrecognized option: " + args[i]);
                // CHECKSTYLE:ON
                printUsage();
                return 1;
            }
        }

        // Supply default values for unspecified arguments.
        if (null == outputDirectoryStr) {
            outputDirectory = filePath.getParent();
        } else {
            outputDirectory = new Path(outputDirectoryStr);
        }
        if (null == compressorName) {
            codec = new DefaultCodec();
        } else {
            Class<?> codecClass = Class.forName(compressorName);
            codec = (CompressionCodec)
              ReflectionUtils.newInstance(codecClass, getConf());
        }
        if (codec instanceof Configurable) {
            ((Configurable) codec).setConf(getConf());
        }

        // Do not write a .splits file so as not to confuse BSONSplitter.
        // Each compressed file will be its own split.
        MongoConfigUtil.setBSONWriteSplits(getConf(), false);

        // Open the file.
        FileSystem inputFS =
          FileSystem.get(filePath.toUri(), getConf());
        FileSystem outputFS =
          FileSystem.get(outputDirectory.toUri(), getConf());
        FSDataInputStream inputStream = inputFS.open(filePath);

        // Use BSONSplitter to split the file.
        Path splitFilePath = getSplitsFilePath(filePath, getConf());
        try {
            loadSplitsFromSplitFile(
              inputFS.getFileStatus(filePath), splitFilePath);
        } catch (NoSplitFileException e) {
            LOG.info("did not find .splits file in " + splitFilePath.toUri());
            setInputPath(filePath);
            readSplits();
        }
        List<BSONFileSplit> splits = getAllSplits();
        LOG.info("compressing " + splits.size() + " splits.");

        byte[] buf = new byte[1024 * 1024];
        for (int i = 0; i < splits.size(); ++i) {
            // e.g., hdfs:///user/hive/warehouse/mongo/OutputFile-42.bz2
            Path splitOutputPath = new Path(
              outputDirectory,
              filePath.getName()
                + "-" + i
                + codec.getDefaultExtension());

            // Compress the split into a new file.
            compressor = CodecPool.getCompressor(codec);
            CompressionOutputStream compressionOutputStream = null;
            try {
                compressionOutputStream = codec.createOutputStream(
                  outputFS.create(splitOutputPath),
                  compressor);
                int totalBytes = 0, bytesRead = 0;
                BSONFileSplit split = splits.get(i);
                inputStream.seek(split.getStart());
                LOG.info("writing " + splitOutputPath.toUri() + ".");
                while (totalBytes < split.getLength() && bytesRead >= 0) {
                    bytesRead = inputStream.read(
                      buf,
                      0,
                      (int) Math.min(buf.length, split.getLength() - totalBytes));
                    if (bytesRead > 0) {
                        compressionOutputStream.write(buf, 0, bytesRead);
                        totalBytes += bytesRead;
                    }
                }
            } finally {
                if (compressionOutputStream != null) {
                    compressionOutputStream.close();
                }
                CodecPool.returnCompressor(compressor);
            }
        }
        LOG.info("done.");

        return 0;
    }

    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new BSONSplitter(), args));
    }

}
