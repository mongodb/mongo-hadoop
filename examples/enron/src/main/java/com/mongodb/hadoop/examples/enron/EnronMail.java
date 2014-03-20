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


import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;

public class EnronMail extends MongoTool {
    public EnronMail() {
        /*
        "mongo.job.input.format=com.mongodb.hadoop.MongoInputFormat",
                    "mongo.input.uri=mongodb://localhost:27017/mongo_hadoop.messages",
        
                    ,
        
                    "mongo.job.mapper=com.mongodb.hadoop.examples.enron.EnronMailMapper",
                    "mongo.job.reducer=com.mongodb.hadoop.examples.enron.EnronMailReducer",
                    //"mongo.job.combiner=com.mongodb.hadoop.examples.enron.EnronMailReducer",
        
                    "mongo.job.output.key=com.mongodb.hadoop.examples.enron.MailPair",
                    "mongo.job.output.value=org.apache.hadoop.io.IntWritable",
        
                    "mongo.job.mapper.output.key=com.mongodb.hadoop.examples.enron.MailPair",
                    "mongo.job.mapper.output.value=org.apache.hadoop.io.IntWritable",
        
                    "mongo.output.uri=mongodb://localhost:27017/mongo_hadoop.message_pairs",
                    "mongo.job.output.format=com.mongodb.hadoop.MongoOutputFormat"
         */
        Configuration conf = new Configuration();
        MongoConfig config = new MongoConfig(conf);
        setConf(conf);
     
        config.setInputFormat(MongoInputFormat.class);
        config.setInputURI("mongodb://localhost:27017/mongo_hadoop.messages");
        config.setMapper(EnronMailMapper.class);
        config.setReducer(EnronMailReducer.class);
//        config.setCombiner(EnronMailReducer.class);
        config.setMapperOutputKey(MailPair.class);
        config.setMapperOutputValue(IntWritable.class);
        config.setOutputKey(MailPair.class);
        config.setOutputValue(IntWritable.class);
        config.setOutputURI("mongodb://localhost:27017/mongo_hadoop.message_pairs");
        config.setOutputFormat(MongoOutputFormat.class);
    }

    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new EnronMail(), pArgs));
    }
}

