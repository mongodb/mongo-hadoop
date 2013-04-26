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
import com.mongodb.hadoop.io.BSONWritable;

// Commons
import org.apache.commons.logging.*;

// Hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;

// Java
import java.io.*;
import java.util.*;

/**
 * The treasury yield reducer.
 */
public class TreasuryYieldReducerV2
    extends MapReduceBase
    implements Reducer<IntWritable, DoubleWritable, IntWritable, BSONWritable> {

    public void reduce(IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, BSONWritable> output, Reporter reporter) throws IOException {
        int count = 0;
        double sum = 0;
        while(values.hasNext()){
            DoubleWritable value = values.next();
            sum += value.get();
            count++;
        }

        final double avg = sum / count;

        LOG.debug( "V2: Average 10 Year Treasury for " + key.get() + " was " + avg );
        BasicBSONObject outputObj = new BasicBSONObject();
        outputObj.put("count", count);
        outputObj.put("avg", avg);
        outputObj.put("sum", sum);
        output.collect( key, new BSONWritable( outputObj ) );
    }

    private static final Log LOG = LogFactory.getLog( TreasuryYieldMapperV2.class );
}

