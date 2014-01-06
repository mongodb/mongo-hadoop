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

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

/**
 * The treasury yield xml config object.
 */
public class TreasuryYieldXMLConfigV2 extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(TreasuryYieldXMLConfigV2.class);

    static class TreasuryYieldMapperV2 extends MapReduceBase implements Mapper<BSONWritable, BSONWritable, IntWritable, DoubleWritable> {

        @Override
        public void map(final BSONWritable key, final BSONWritable value, final OutputCollector<IntWritable, DoubleWritable> output,
                        final Reporter reporter)
            throws IOException {
            final int year = ((Date) value.getDoc().get("_id")).getYear() + 1900;
            double bid10Year = ((Number) value.getDoc().get("bc10Year")).doubleValue();
            output.collect(new IntWritable(year), new DoubleWritable(bid10Year));
        }

    }

    static class TreasuryYieldReducerV2 extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, BSONWritable> {

        @Override
        public void reduce(final IntWritable key, final Iterator<DoubleWritable> values,
                           final OutputCollector<IntWritable, BSONWritable> output, final Reporter reporter) throws IOException {
            int count = 0;
            double sum = 0;
            while (values.hasNext()) {
                DoubleWritable value = values.next();
                sum += value.get();
                count++;
            }

            final double avg = sum / count;

            LOG.info("V2: Average 10 Year Treasury for " + key.get() + " was " + avg);
            BasicBSONObject outputObj = new BasicBSONObject();
            outputObj.put("count", count);
            outputObj.put("avg", avg);
            outputObj.put("sum", sum);
            output.collect(key, new BSONWritable(outputObj));
        }

    }

    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final JobConf job = new JobConf(conf);
        job.setReducerClass(TreasuryYieldReducerV2.class);
        job.setMapperClass(TreasuryYieldMapperV2.class);
        job.setOutputFormat(com.mongodb.hadoop.mapred.MongoOutputFormat.class);
        job.setOutputKeyClass(MongoConfigUtil.getOutputKey(conf));
        job.setOutputValueClass(MongoConfigUtil.getOutputValue(conf));
        job.setMapOutputKeyClass(MongoConfigUtil.getMapperOutputKey(conf));
        job.setMapOutputValueClass(MongoConfigUtil.getMapperOutputValue(conf));
        job.setInputFormat(com.mongodb.hadoop.mapred.MongoInputFormat.class);
        JobClient.runJob(job);
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        ToolRunner.run(new TreasuryYieldXMLConfigV2(), args);
    }
}