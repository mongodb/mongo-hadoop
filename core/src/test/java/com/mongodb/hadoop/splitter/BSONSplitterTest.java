package com.mongodb.hadoop.splitter;

import com.mongodb.hadoop.input.BSONFileSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.hadoop.bookstore.BookstoreTest.INVENTORY_BSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class BSONSplitterTest {

    private static final BSONSplitter SPLITTER = new BSONSplitter();
    private static FileSystem fs;
    private static FileStatus file;
    private static Configuration conf;

    @BeforeClass
    public static void setUpClass() throws IOException {
        conf = new Configuration();
        MongoConfigUtil.setInputKey(conf, "customKeyName");
        // Don't actually write a split file.
        MongoConfigUtil.setBSONWriteSplits(conf, false);
        SPLITTER.setConf(conf);
        fs = FileSystem.getLocal(conf);
        file = fs.getFileStatus(
          new Path(
            BSONSplitterTest.class.getResource(
              "/bookstore-dump/inventory.bson").getPath()));

        // Remove the splits file if it exists.
        Path splitsFilePath = BSONSplitter.getSplitsFilePath(
          file.getPath(), conf);
        fs.delete(splitsFilePath, false);
    }

    /**
     * Assert that the given BSONFileSplit comes from the local filesystem,
     * is configured properly, and contains the entire test file.
     * @param split the split to test
     * @throws IOException
     */
    public void assertOneSplit(final BSONFileSplit split) throws IOException {
        assertEquals(0, split.getStart());
        assertEquals(file.getLen(), split.getLength());
        assertEquals(1, split.getLocations().length);
        assertEquals("localhost", split.getLocations()[0]);
        assertEquals("customKeyName", split.getKeyField());
    }

    /**
     * Assert that two lists of BSONFileSplits are equal.
     *
     * @param splits1
     * @param splits2
     * @throws IOException
     */
    public void assertSplitsEqual(
      final List<BSONFileSplit> splits1, final List<BSONFileSplit> splits2)
      throws IOException {
        assertEquals(splits1.size(), splits2.size());
        for (int i = 0; i < splits1.size(); i++) {
            BSONFileSplit split1 = splits1.get(i);
            BSONFileSplit split2 = splits2.get(i);

            assertEquals(split1.getPath(), split2.getPath());
            assertEquals(split1.getStart(), split2.getStart());
            assertEquals(split1.getLength(), split2.getLength());
            List<String> split1Locations = Arrays.asList(split1.getLocations());
            List<String> split2Locations = Arrays.asList(split2.getLocations());
            assertEquals(split1Locations, split2Locations);
        }
    }

    @Test
    public void testGetSplitsFilePath() {
        Configuration conf = new Configuration();
        Path bsonFilePath = new Path("data.bson");

        // No explicit configuration.
        assertEquals(
          new Path(".data.bson.splits"),
          BSONSplitter.getSplitsFilePath(bsonFilePath, conf)
        );

        // Explicit configuration.
        MongoConfigUtil.setBSONSplitsPath(conf, "/foo/bar");
        assertEquals(
          new Path("/foo/bar/.data.bson.splits"),
          BSONSplitter.getSplitsFilePath(bsonFilePath, conf)
        );
    }

    @Test
    public void testGetSplitSize() {
        Configuration ssConf = new Configuration();

        // Provide old-style split size options.
        ssConf.set("mapred.max.split.size", "1000");
        ssConf.set("dfs.blockSize", "12345");

        // No input file, so should use max split size from the configuration.
        assertEquals(1000L, BSONSplitter.getSplitSize(ssConf, null));

        // Prefer the smaller of the file block size and max configured size.
        ssConf.set("mapred.max.split.size", "10000000000000");
        assertEquals(12345L, BSONSplitter.getSplitSize(ssConf, null));
        assertEquals(
          file.getBlockSize(),
          BSONSplitter.getSplitSize(ssConf, file));

        // Prefer block size on the file itself over global block size.
        assertEquals(
          file.getBlockSize(),
          BSONSplitter.getSplitSize(ssConf, file));

        // Use larger of min size and max or block size.
        ssConf.set("mapred.min.split.size", "100000000000000000");
        assertEquals(
          100000000000000000L,
          BSONSplitter.getSplitSize(ssConf, null));
        assertEquals(
          100000000000000000L,
          BSONSplitter.getSplitSize(ssConf, file));

        // New-style configuration option shadows the old one.
        ssConf.set("mapreduce.input.fileinputformat.split.maxsize", "5000");
        ssConf.set("mapreduce.input.fileinputformat.split.minsize", "1");
        assertEquals(5000L, BSONSplitter.getSplitSize(ssConf, null));
        assertEquals(5000L, BSONSplitter.getSplitSize(ssConf, file));
    }

    @Test
    public void testGetStartingPositionForSplit() throws IOException {
        String inventoryPathString = INVENTORY_BSON.toString();
        Path inventoryPath = new Path(inventoryPathString);
        Configuration conf = new Configuration();
        BSONSplitter splitter = new BSONSplitter();
        splitter.setInputPath(inventoryPath);
        // This is a very small value for maxsize and will result in many
        // splits being created.
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 2000L);
        splitter.setConf(conf);

        // Test without splits file.
        FileSplit fileSplit = new FileSplit(
          inventoryPath, 2000L, 100L, new String[]{}
        );
        // Writing the split file is enabled by default, so this will cause
        // the BSONSplitter to create the splits file for later tests.
        assertEquals(2130L, splitter.getStartingPositionForSplit(fileSplit));

        // Test with splits file, which was created by the previous call to
        // getStartingPositionForSplit.
        assertEquals(2130L, splitter.getStartingPositionForSplit(fileSplit));

        // Test with reading the splits file disabled.
        MongoConfigUtil.setBSONReadSplits(conf, false);
        assertEquals(2130L, splitter.getStartingPositionForSplit(fileSplit));
    }

    @Test
    public void testCreateFileSplit() throws IOException {
        BSONFileSplit splitResult = SPLITTER.createFileSplit(
          file, fs, 0, file.getLen());
        assertOneSplit(splitResult);
    }

    @Test
    public void testCreateFileSplitFromBSON() throws IOException {
        BSONObject splitSpec = new BasicBSONObject();
        splitSpec.put("s", 0L);
        splitSpec.put("l", file.getLen());

        BSONFileSplit splitResult = SPLITTER.createFileSplitFromBSON(
          splitSpec, fs, file);
        assertOneSplit(splitResult);
    }

    @Test
    public void testReadSplitsForFile() throws IOException {
        Configuration readSplitsConfig = new Configuration(conf);
        SPLITTER.setConf(readSplitsConfig);

        // Only one split if reading splits is disabled.
        MongoConfigUtil.setBSONReadSplits(readSplitsConfig, false);
        SPLITTER.readSplitsForFile(file);
        List<BSONFileSplit> splitsList = SPLITTER.getAllSplits();
        assertEquals(1, splitsList.size());
        BSONFileSplit theSplit = splitsList.get(0);
        assertOneSplit(theSplit);

        // Actually compute splits.
        MongoConfigUtil.setBSONReadSplits(readSplitsConfig, true);
        // Set split size to be really small so we get a lot of them.
        readSplitsConfig.set(
          "mapreduce.input.fileinputformat.split.maxsize", "5000");
        SPLITTER.readSplitsForFile(file);
        splitsList = SPLITTER.getAllSplits();

        // Value found through manual inspection.
        assertEquals(40, splitsList.size());

        // Make sure that all splits start on document boundaries.
        FSDataInputStream stream = fs.open(file.getPath());
        BSONDecoder decoder = new BasicBSONDecoder();
        BSONCallback callback = new BasicBSONCallback();
        for (BSONFileSplit split : splitsList) {
            stream.seek(split.getStart());
            decoder.decode(stream, callback);
            BSONObject doc = (BSONObject) callback.get();
            assertTrue(doc.containsField("_id"));
        }
    }

    @Test
    public void testReadSplits() throws IOException {
        SPLITTER.setInputPath(null);
        try {
            SPLITTER.readSplits();
            fail("Need to set inputPath to call readSplits.");
        } catch (IllegalStateException e) {
            // ok.
        }

        SPLITTER.readSplitsForFile(file);
        List<BSONFileSplit> expectedSplits = SPLITTER.getAllSplits();

        SPLITTER.setInputPath(file.getPath());
        SPLITTER.readSplits();
        List<BSONFileSplit> actualSplits = SPLITTER.getAllSplits();

        assertSplitsEqual(expectedSplits, actualSplits);
    }

    @Test
    public void testWriteAndLoadSplits() throws IOException {
        Configuration writeConfig = new Configuration(conf);
        MongoConfigUtil.setBSONWriteSplits(writeConfig, true);
        SPLITTER.setConf(writeConfig);
        SPLITTER.setInputPath(file.getPath());
        Path splitsFilePath = BSONSplitter.getSplitsFilePath(
          file.getPath(), writeConfig);

        try {
            // readSplitsForFile has the side-effect of writing the splits file.
            SPLITTER.readSplitsForFile(file);
            List<BSONFileSplit> expectedSplits = SPLITTER.getAllSplits();

            try {
                SPLITTER.loadSplitsFromSplitFile(file, splitsFilePath);
            } catch (BSONSplitter.NoSplitFileException nsfe) {
                fail("Splits file not created.");
            }

            assertSplitsEqual(expectedSplits, SPLITTER.getAllSplits());
        } finally {
            SPLITTER.setConf(conf);
            // Remove the splits file.
            fs.delete(splitsFilePath, false);
        }
    }
}
