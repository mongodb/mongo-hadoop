/*
 * Copyright 2011 10gen Inc.
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
package com.mongodb.hadoop.examples.enron;


import com.mongodb.hadoop.BSONFileInputFormat;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;

public class EnronMail extends MongoTool {
    public EnronMail() {
        JobConf conf = new JobConf(new Configuration());
        if (MongoTool.isMapRedV1()) {
            MapredMongoConfigUtil.setInputFormat(conf,
              com.mongodb.hadoop.mapred.BSONFileInputFormat.class);
            MapredMongoConfigUtil.setOutputFormat(conf,
              com.mongodb.hadoop.mapred.MongoOutputFormat.class);
        } else {
            MongoConfigUtil.setInputFormat(conf, BSONFileInputFormat.class);
            MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);
        }
        FileInputFormat.addInputPath(conf, new Path("/messages"));
        MongoConfig config = new MongoConfig(conf);
        config.setInputKey("headers.From");
        config.setMapper(EnronMailMapper.class);
        config.setReducer(EnronMailReducer.class);
        config.setMapperOutputKey(MailPair.class);
        config.setMapperOutputValue(IntWritable.class);
        config.setOutputKey(MailPair.class);
        config.setOutputValue(IntWritable.class);
        config.setOutputURI(
          "mongodb://localhost:27017/mongo_hadoop.message_pairs");
        setConf(conf);
    }

    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new EnronMail(), pArgs));
    }
}

