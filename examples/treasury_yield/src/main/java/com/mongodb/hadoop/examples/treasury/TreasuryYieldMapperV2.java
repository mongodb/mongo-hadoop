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

import org.bson.BSONObject;
import com.mongodb.hadoop.util.*;

// Commons
import org.apache.commons.logging.*;

// Hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;

import com.mongodb.hadoop.io.BSONWritable;
// Java
import java.io.IOException;
import java.util.Date;

/**
 * The treasury yield mapper.
 */
public class TreasuryYieldMapperV2 
    extends MapReduceBase
    implements Mapper<Date, BSONObject, IntWritable, DoubleWritable> {

    @Override
    public void map(Date key, BSONObject value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {

        LOG.debug("mapping: " + value.get("_id"));
        final int year = key.getYear() + 1900;
        double bid10Year = ( (Number) value.get( "bc10Year" ) ).doubleValue();
        output.collect( new IntWritable( year ), new DoubleWritable( bid10Year ) );
    }

    private static final Log LOG = LogFactory.getLog( TreasuryYieldReducerV2.class );
}

