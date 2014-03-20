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
package com.mongodb.hadoop.examples.treasury;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;

/**
 * The treasury yield xml config object.
 */
public class TreasuryYieldXMLConfig extends MongoTool {

    static {
        Configuration.addDefaultResource("src/examples/hadoop-local.xml");
        Configuration.addDefaultResource("src/examples/mongo-defaults.xml");
    }

    public TreasuryYieldXMLConfig() {
        Configuration conf = new Configuration();
        MongoConfig config = new MongoConfig(conf);
        config.setInputFormat(com.mongodb.hadoop.MongoInputFormat.class);
        config.setInputURI("mongodb://localhost:27017/mongo_hadoop.yield_historical.in");
        
        config.setMapper(TreasuryYieldMapper.class);
        config.setMapperOutputKey(IntWritable.class);
        config.setMapperOutputValue(DoubleWritable.class);
        
        config.setReducer(TreasuryYieldReducer.class);
        config.setOutputKey(IntWritable.class);
        config.setOutputValue(BSONWritable.class);
        config.setOutputURI("mongodb://localhost:27017/mongo_hadoop.yield_historical.out");
        config.setOutputFormat(MongoOutputFormat.class);
        
        setConf(conf);
    }

    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new TreasuryYieldXMLConfig(), pArgs));
    }
}