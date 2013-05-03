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

// Mongo

import org.bson.*;
import com.mongodb.hadoop.util.*;
import com.mongodb.hadoop.io.*;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;

// Hadoop
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 * The treasury yield xml config object.
 */
public class TreasuryYieldXMLConfigV2 extends Configured implements Tool{

    static class TreasuryYieldMapperV2 
        extends MapReduceBase
        implements Mapper<BSONWritable, BSONWritable, IntWritable, DoubleWritable> {

        @Override
        public void map(BSONWritable key, BSONWritable value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
            final int year = ((Date)value.getDoc().get("_id")).getYear() + 1900;
            //final int year = key.getYear() + 1900;
            double bid10Year = ( (Number) value.getDoc().get( "bc10Year" ) ).doubleValue();
            output.collect( new IntWritable( year ), new DoubleWritable( bid10Year ) );
        }

    }

    static class TreasuryYieldReducerV2
        extends MapReduceBase
        implements Reducer<IntWritable, DoubleWritable, IntWritable, BSONWritable> {

        @Override
        public void reduce(IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, BSONWritable> output, Reporter reporter) throws IOException {
            int count = 0;
            double sum = 0;
            while(values.hasNext()){
                DoubleWritable value = values.next();
                sum += value.get();
                count++;
            }

            final double avg = sum / count;

            System.out.println( "V2: Average 10 Year Treasury for " + key.get() + " was " + avg );
            BasicBSONObject outputObj = new BasicBSONObject();
            outputObj.put("count", count);
            outputObj.put("avg", avg);
            outputObj.put("sum", sum);
            output.collect( key, new BSONWritable( outputObj ) );
        }

    }

    public int run(String args[]) throws Exception{
        final Configuration conf = getConf();
        final JobConf job = new JobConf( conf );
        job.setReducerClass( TreasuryYieldReducerV2.class );
        job.setMapperClass( TreasuryYieldMapperV2.class );
        job.setOutputFormat( com.mongodb.hadoop.mapred.MongoOutputFormat.class );
        job.setOutputKeyClass( MongoConfigUtil.getOutputKey( conf ) );
        job.setOutputValueClass( MongoConfigUtil.getOutputValue( conf ) );
        job.setInputFormat( com.mongodb.hadoop.mapred.MongoInputFormat.class );
        JobClient.runJob(job);
        return 0;
    }

    public static void main(String args[]) throws Exception{
        ToolRunner.run(new TreasuryYieldXMLConfigV2(), args );
    }

    private static final Log LOG = LogFactory.getLog( TreasuryYieldXMLConfigV2.class );

}

