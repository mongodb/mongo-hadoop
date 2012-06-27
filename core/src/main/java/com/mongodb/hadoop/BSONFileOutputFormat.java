package com.mongodb.hadoop;

import com.mongodb.hadoop.io.BSONFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

 public class BSONFileOutputFormat<K, V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter( TaskAttemptContext context )
           throws IOException, InterruptedException {

     final Configuration conf = context.getConfiguration();

     /* TODO - Look into supporting compression, though mongoimport can't read from it
      *        ... it may be useful for people feeding data further through Hadoop, etc.
      */

     // get the path of the temporary output file
     final Path file = getDefaultWorkFile(context, "");
     final FileSystem fs = file.getFileSystem(conf);

     final BSONFile.Writer out = new BSONFile.Writer<K, V>(fs, conf, file);

     return new RecordWriter<K, V>() {

         public void write(K key, V value) throws IOException {
           out.append(key, value);
         }

        public void close(TaskAttemptContext context) throws IOException {
          out.close();
        }
      };
    }

    private static final Log log = LogFactory.getLog(BSONFileOutputFormat.class);

}
