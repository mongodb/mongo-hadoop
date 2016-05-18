package com.mongodb.hadoop;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.hadoop.input.GridFSSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.types.ObjectId;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GridFSInputFormat
  extends InputFormat<NullWritable, BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(GridFSInputFormat.class);

    @Override
    public List<InputSplit> getSplits(final JobContext context)
      throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        DBCollection inputCollection =
          MongoConfigUtil.getInputCollection(conf);
        MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);

        GridFS gridFS = new GridFS(
          inputCollection.getDB(),
          inputCollection.getName());

        DBObject query = MongoConfigUtil.getQuery(conf);
        List<InputSplit> splits = new LinkedList<InputSplit>();
        for (GridFSDBFile file : gridFS.find(query)) {
            // One split per file.
            if (MongoConfigUtil.isGridFSWholeFileSplit(conf)) {
                splits.add(
                  new GridFSSplit(
                    inputURI,
                    (ObjectId) file.getId(),
                    (int) file.getChunkSize(),
                    file.getLength()));
            }
            // One split per file chunk.
            else {
                for (int chunk = 0; chunk < file.numChunks(); ++chunk) {
                    splits.add(
                      new GridFSSplit(
                        inputURI,
                        (ObjectId) file.getId(),
                        (int) file.getChunkSize(),
                        file.getLength(),
                        chunk));
                }
            }
        }

        LOG.debug("Found GridFS splits: " + splits);
        return splits;
    }

    @Override
    public RecordReader<NullWritable, BinaryComparable>
    createRecordReader(final InputSplit split, final TaskAttemptContext context)
      throws IOException, InterruptedException {
        if (MongoConfigUtil.isGridFSReadBinary(context.getConfiguration())) {
            // Read GridFS files as binary files.
            return new GridFSBinaryRecordReader();
        } else {
            // Read GridFS files as text.
            return new GridFSTextRecordReader();
        }
    }

    static class GridFSBinaryRecordReader
      extends RecordReader<NullWritable, BinaryComparable> {
        private final BytesWritable bw = new BytesWritable();
        private GridFSSplit split;
        private InputStream stream;
        private boolean readLast;
        private byte[] buff;

        @Override
        public void initialize(
          final InputSplit split, final TaskAttemptContext context)
          throws IOException, InterruptedException {
            this.split = (GridFSSplit) split;
            readLast = false;
            buff = new byte[1024 * 1024 * 16];
            stream = this.split.getData();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            // Read the whole split once.
            if (readLast) {
                return false;
            }

            int totalBytes = 0, bytesRead;
            do {
                bytesRead = stream.read(
                  buff, totalBytes, buff.length - totalBytes);
                if (bytesRead > 0) {
                    totalBytes += bytesRead;
                }
            } while (bytesRead > 0);
            bw.set(buff, 0, totalBytes);
            readLast = true;
            return true;
        }

        @Override
        public NullWritable getCurrentKey()
          throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public BytesWritable getCurrentValue()
          throws IOException, InterruptedException {
            return bw;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return readLast ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    static class ChunkReadingCharSequence implements CharSequence, Closeable {
        private Reader reader;
        private int chunkSize;
        private int length;
        private StringBuilder builder;
        private char[] buff;

        public ChunkReadingCharSequence(final GridFSSplit split)
          throws IOException {
            this.reader = new BufferedReader(
              new InputStreamReader(split.getData()));
            this.chunkSize = split.getChunkSize();
            builder = new StringBuilder();
            buff = new char[1024 * 1024 * 16];
            // How many more bytes can be read starting from this chunk?
            length = (int) split.getLength() - split.getChunkId() * chunkSize;
        }

        @Override
        public int length() {
            return length;
        }

        private void advanceToIndex(final int index) throws IOException {
            if (index >= builder.length()) {
                while (index >= builder.length()) {
                    int bytesRead = reader.read(buff);
                    if (bytesRead > 0) {
                        builder.append(buff, 0, bytesRead);
                    } else {
                        break;
                    }
                }
            }
        }

        @Override
        public char charAt(final int index) {
            try {
                advanceToIndex(index);
            } catch (IOException e) {
                throw new IndexOutOfBoundsException(
                  "Could not advance stream to index: "
                    + index + "; reason: " + e.getMessage());
            }
            return builder.charAt(index);
        }

        @Override
        public CharSequence subSequence(final int start, final int end) {
            try {
                advanceToIndex(end);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return builder.subSequence(start, end);
        }

        /**
         * Get the entire contents of this GridFS chunk.
         * @return the contents of the chunk as a CharSequence (a String).
         */
        public CharSequence chunkContents() {
            return subSequence(0, Math.min(chunkSize, length));
        }

        public CharSequence fileContents() {
            return subSequence(0, length);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    static class GridFSTextRecordReader
      extends RecordReader<NullWritable, BinaryComparable> {

        private GridFSSplit split;
        private final Text text = new Text();
        private int totalMatches = 0;
        private long chunkSize;
        private boolean readLast;
        private boolean readWholeFile;
        private Pattern delimiterPattern;
        private Matcher matcher;
        private int previousMatchIndex = 0;
        private ChunkReadingCharSequence chunkData;

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context)
          throws IOException, InterruptedException {
            this.split = (GridFSSplit) split;
            Configuration conf = context.getConfiguration();

            String patternString =
              MongoConfigUtil.getGridFSDelimiterPattern(conf);
            chunkSize = this.split.getChunkSize();
            chunkData = new ChunkReadingCharSequence(this.split);
            readLast = false;
            readWholeFile = MongoConfigUtil.isGridFSWholeFileSplit(conf);
            if (!(null == patternString || patternString.isEmpty())) {
                delimiterPattern = Pattern.compile(patternString);
                matcher = delimiterPattern.matcher(chunkData);

                // Skip past the first delimiter if this is not the first chunk.
                if (this.split.getChunkId() > 0) {
                    nextToken();
                }
            }
        }

        private CharSequence nextToken() {
            if (matcher.find()) {
                CharSequence slice = chunkData.subSequence(
                  previousMatchIndex, matcher.start());
                // Skip the delimiter.
                previousMatchIndex = matcher.end();
                return slice;
            }
            // Last token after the final delimiter.
            readLast = true;
            return chunkData.subSequence(
              previousMatchIndex, chunkData.length());
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (readLast) {
                LOG.debug("skipping the rest of this chunk because we've "
                    + "read beyond the end: " + previousMatchIndex
                    + "; read " + totalMatches + " matches here.");
                return false;
            }

            // No delimiter being used, and we haven't returned anything yet.
            if (null == matcher) {
                if (readWholeFile) {
                    text.set(chunkData.fileContents().toString());
                } else {
                    text.set(chunkData.chunkContents().toString());
                }
                ++totalMatches;
                readLast = true;
                return true;
            }

            // Delimiter used; do we have more matches?
            CharSequence nextToken = nextToken();
            if (nextToken != null) {
                // Read one more token past the end of the split.
                if (!readWholeFile && previousMatchIndex >= chunkSize) {
                    readLast = true;
                }
                text.set(nextToken.toString());
                ++totalMatches;
                return true;
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Read " + totalMatches + " segments.");
            }

            // No match.
            return false;
        }

        @Override
        public NullWritable getCurrentKey()
          throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return text;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) Math.min(
              previousMatchIndex / (float) chunkSize, 1.0);
        }

        @Override
        public void close() throws IOException {
            chunkData.close();
        }
    }
}
