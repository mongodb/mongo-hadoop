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

package com.mongodb.hadoop.hive.input;

import com.mongodb.hadoop.hive.MongoStorageHandler;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.input.MongoRecordReader;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.MongoSplitterFactory;
import com.mongodb.hadoop.splitter.SplitFailedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/*
 * Defines a HiveInputFormat for use in reading data from MongoDB into a hive table
 * 
 */
public class HiveMongoInputFormat extends HiveInputFormat<BSONWritable, BSONWritable> {

    private static final Log LOG = LogFactory.getLog(HiveMongoInputFormat.class);

    @Override
    public RecordReader<BSONWritable, BSONWritable> getRecordReader(final InputSplit split,
                                                                    final JobConf conf,
                                                                    final Reporter reporter)
        throws IOException {

        // split is of type 'MongoHiveInputSplit'
        MongoHiveInputSplit mhis = (MongoHiveInputSplit) split;

        // return MongoRecordReader. Delegate is of type 'MongoInputSplit'
        return new MongoRecordReader((MongoInputSplit) mhis.getDelegate());
    }

    @Override
    public FileSplit[] getSplits(final JobConf conf, final int numSplits)
        throws IOException {

        try {
            MongoSplitter splitterImpl = MongoSplitterFactory.getSplitter(conf);
            final List<org.apache.hadoop.mapreduce.InputSplit> splits =
                splitterImpl.calculateSplits();
            InputSplit[] splitIns = splits.toArray(new InputSplit[splits.size()]);

            // wrap InputSplits in FileSplits so that 'getPath' 
            // doesn't produce an error (Hive bug)
            FileSplit[] wrappers = new FileSplit[splitIns.length];
            Path path = new Path(conf.get(MongoStorageHandler.TABLE_LOCATION));
            for (int i = 0; i < wrappers.length; i++) {
                wrappers[i] = new MongoHiveInputSplit(splitIns[i], path);
            }

            return wrappers;
        } catch (SplitFailedException spfe) {
            // split failed because no namespace found 
            // (so the corresponding collection doesn't exist)
            LOG.error(spfe.getMessage(), spfe);
            throw new IOException(spfe.getMessage(), spfe);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /*
     * MongoHiveInputSplit ->
     *  Used to wrap MongoInputSplits (as a delegate) to by-pass the Hive bug where
     *  'HiveInputSplit.getPath' is always called.
     */
    public static class MongoHiveInputSplit extends FileSplit {
        private InputSplit delegate;
        private Path path;

        MongoHiveInputSplit() {
            this(new MongoInputSplit());
        }

        MongoHiveInputSplit(final InputSplit delegate) {
            this(delegate, null);
        }

        MongoHiveInputSplit(final InputSplit delegate, final Path path) {
            super(path, 0, 0, (String[]) null);
            this.delegate = delegate;
            this.path = path;
        }

        public InputSplit getDelegate() {
            return delegate;
        }

        @Override
        public long getLength() {
            return 1L;
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            Text.writeString(out, path.toString());
            delegate.write(out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            path = new Path(Text.readString(in));
            delegate.readFields(in);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public Path getPath() {
            return path;
        }
    }
}
