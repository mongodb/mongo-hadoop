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
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;

/**
 * The treasury yield xml config object.
 */
public class TreasuryYieldXMLConfig extends MongoTool {
    private static final Log LOG = LogFactory.getLog(TreasuryYieldXMLConfig.class);

    public TreasuryYieldXMLConfig() {
        this(new Configuration());
    }

    public TreasuryYieldXMLConfig(final Configuration conf) {
        MongoConfig config = new MongoConfig(conf);
        setConf(conf);

        config.setInputFormat(MongoInputFormat.class);

        config.setMapper(TreasuryYieldMapper.class);
        config.setMapperOutputKey(IntWritable.class);
        config.setMapperOutputValue(DoubleWritable.class);

        config.setReducer(TreasuryYieldReducer.class);
        config.setOutputKey(IntWritable.class);
        config.setOutputValue(BSONWritable.class);
        config.setOutputFormat(MongoOutputFormat.class);
    }

    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new TreasuryYieldXMLConfig(), pArgs));
    }
}