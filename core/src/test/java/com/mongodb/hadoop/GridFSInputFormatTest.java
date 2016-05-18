package com.mongodb.hadoop;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GridFSInputFormatTest extends BaseHadoopTest {

    private static MongoClient client = new MongoClient();
    private static GridFSInputFormat inputFormat =
      new GridFSInputFormat();
    private static String[] readmeSections;
    private static GridFSBucket bucket = GridFSBuckets.create(
      client.getDatabase("mongo_hadoop"));
    private static StringBuilder fileContents;
    private static GridFSFile readme;
    private static GridFSFile bson;

    private static void uploadFile(final File file)
      throws IOException {
        // Set a small chunks size so we get multiple chunks per readme.
        GridFSUploadStream gridfsStream = bucket.openUploadStream(
          file.getName(), new GridFSUploadOptions().chunkSizeBytes(1024));
        IOUtils.copy(new FileInputStream(file), gridfsStream);
        gridfsStream.close();
    }

    private static void cleanFile(final String filename) {
        bucket.find(new Document("filename", filename)).forEach(
          new Block<GridFSFile>() {
              @Override
              public void apply(final GridFSFile gridFSFile) {
                  bucket.delete(gridFSFile.getObjectId());
              }
          }
        );
    }

    @BeforeClass
    public static void setUpClass() throws IOException, URISyntaxException {
        // Clean up files and re-upload them.
        cleanFile("README.md");
        cleanFile("orders.bson");
        File bsonFile = new File(GridFSInputFormatTest.class.getResource(
          "/bookstore-dump/orders.bson").toURI().getPath());
        uploadFile(bsonFile);
        File readmeFile = new File(PROJECT_HOME, "README.md");
        uploadFile(readmeFile);

        // Read the README, preparing to count sections and upload to GridFS.
        fileContents = new StringBuilder();
        BufferedReader reader = new BufferedReader(new FileReader(readmeFile));
        int charsRead;
        do {
            char[] buff = new char[1024];
            charsRead = reader.read(buff);
            if (charsRead > 0) {
                fileContents.append(buff, 0, charsRead);
            }
        } while (charsRead > 0);
        // Count number of sections in the README ("## ...").
        readmeSections = Pattern.compile("#+").split(fileContents);

        readme = bucket.find(new Document("filename", "README.md")).first();
        bson = bucket.find(new Document("filename", "orders.bson")).first();
    }

    @AfterClass
    public static void tearDownClass() {
        cleanFile("README.md");
        cleanFile("orders.bson");
    }

    private static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(
          conf, "mongodb://localhost:27017/mongo_hadoop.fs");
        MongoConfigUtil.setQuery(
          conf, new BasicDBObject("filename", "README.md"));
        return conf;
    }

    private static JobContext mockJobContext(final Configuration conf) {
        JobContext context = mock(JobContext.class);
        when(context.getConfiguration()).thenReturn(conf);
        return context;
    }

    private static TaskAttemptContext mockTaskAttemptContext(
      final Configuration conf) {
        TaskAttemptContext context = mock(TaskAttemptContext.class);
        when(context.getConfiguration()).thenReturn(conf);
        return context;
    }

    private List<InputSplit> getSplits()
      throws IOException, InterruptedException {
        JobContext context = mock(JobContext.class);
        when(context.getConfiguration()).thenReturn(getConfiguration());
        return inputFormat.getSplits(context);
    }

    @Test
    public void testGetSplits() throws IOException, InterruptedException {
        assertEquals(
          (int) Math.ceil(
            readme.getLength() / (float) readme.getChunkSize()),
          getSplits().size());
    }

    @Test
    public void testRecordReader() throws IOException, InterruptedException {
        List<InputSplit> splits = getSplits();
        Configuration conf = getConfiguration();
        // Split README by sections in Markdown.
        MongoConfigUtil.setGridFSDelimiterPattern(conf, "#+");
        TaskAttemptContext context = mockTaskAttemptContext(conf);
        List<String> sections = new ArrayList<String>();
        for (InputSplit split : splits) {
            RecordReader reader = new GridFSInputFormat.GridFSTextRecordReader();
            reader.initialize(split, context);
            while (reader.nextKeyValue()) {
                sections.add(reader.getCurrentValue().toString());
            }
        }
        assertEquals(Arrays.asList(readmeSections), sections);
    }

    @Test
    public void testRecordReaderNoDelimiter()
      throws IOException, InterruptedException {
        List<InputSplit> splits = getSplits();
        Configuration conf = getConfiguration();
        // Empty delimiter == no delimiter.
        MongoConfigUtil.setGridFSDelimiterPattern(conf, "");
        TaskAttemptContext context = mockTaskAttemptContext(conf);
        StringBuilder fileText = new StringBuilder();
        for (InputSplit split : splits) {
            GridFSInputFormat.GridFSTextRecordReader reader =
              new GridFSInputFormat.GridFSTextRecordReader();
            reader.initialize(split, context);
            while (reader.nextKeyValue()) {
                fileText.append(reader.getCurrentValue().toString());
            }
        }
        assertEquals(fileContents.toString(), fileText.toString());
    }

    @Test
    public void testReadWholeFile() throws IOException, InterruptedException {
        Configuration conf = getConfiguration();
        MongoConfigUtil.setGridFSWholeFileSplit(conf, true);

        JobContext jobContext = mockJobContext(conf);
        List<InputSplit> splits = inputFormat.getSplits(jobContext);
        // Empty delimiter == no delimiter.
        MongoConfigUtil.setGridFSDelimiterPattern(conf, "#+");
        TaskAttemptContext context = mockTaskAttemptContext(conf);
        assertEquals(1, splits.size());
        List<String> sections = new ArrayList<String>();
        for (InputSplit split : splits) {
            GridFSInputFormat.GridFSTextRecordReader reader =
              new GridFSInputFormat.GridFSTextRecordReader();
            reader.initialize(split, context);
            int i;
            for (i = 0; reader.nextKeyValue(); ++i) {
                sections.add(reader.getCurrentValue().toString());
            }
        }
        assertEquals(Arrays.asList(readmeSections), sections);
    }

    @Test
    public void testReadWholeFileNoDelimiter()
      throws IOException, InterruptedException {
        Configuration conf = getConfiguration();
        MongoConfigUtil.setGridFSWholeFileSplit(conf, true);

        JobContext jobContext = mockJobContext(conf);

        List<InputSplit> splits = inputFormat.getSplits(jobContext);
        // Empty delimiter == no delimiter.
        MongoConfigUtil.setGridFSDelimiterPattern(conf, "");
        TaskAttemptContext context = mockTaskAttemptContext(conf);
        assertEquals(1, splits.size());
        String fileText = null;
        for (InputSplit split : splits) {
            GridFSInputFormat.GridFSTextRecordReader reader =
              new GridFSInputFormat.GridFSTextRecordReader();
            reader.initialize(split, context);
            int i;
            for (i = 0; reader.nextKeyValue(); ++i) {
                fileText = reader.getCurrentValue().toString();
            }
            assertEquals(1, i);
        }
        assertEquals(fileContents.toString(), fileText);
    }

    @Test
    public void testReadBinaryFiles()
      throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = getConfiguration();
        MongoConfigUtil.setQuery(conf,
          new BasicDBObject("filename", "orders.bson"));
        MongoConfigUtil.setGridFSWholeFileSplit(conf, true);
        MongoConfigUtil.setGridFSReadBinary(conf, true);

        JobContext context = mockJobContext(conf);
        TaskAttemptContext taskContext = mockTaskAttemptContext(conf);

        List<InputSplit> splits = inputFormat.getSplits(context);
        assertEquals(1, splits.size());
        int i = 0;
        byte[] buff = null;
        for (InputSplit split : splits) {
            GridFSInputFormat.GridFSBinaryRecordReader reader =
              new GridFSInputFormat.GridFSBinaryRecordReader();
            reader.initialize(split, taskContext);
            for (; reader.nextKeyValue(); ++i) {
                buff = reader.getCurrentValue().copyBytes();
            }
        }
        // Only one record to read on the split.
        assertEquals(1, i);
        assertNotNull(buff);
        assertEquals(bson.getLength(), buff.length);
    }

}
