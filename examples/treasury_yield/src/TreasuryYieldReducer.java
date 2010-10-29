// TreasuryYieldMapper.java
/*
 * Copyright 2010 10gen Inc.
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
package com.mongodb.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.bson.*;

import com.mongodb.hadoop.util.*;

public class TreasuryYieldReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    private static final Log log = LogFactory.getLog( TreasuryYieldReducer.class );

    public void reduce( IntWritable key , Iterable<DoubleWritable> values , Context context ) throws IOException, InterruptedException{

        int count = 0;
        double sum = 0;
        for ( final DoubleWritable value : values ) {
            log.debug( "Key: " + key + " Value: " + value );
            sum += value.get();
            count++;
        }

        double avg = sum / count;

        log.info( "Average 10 Year Treasury for " + key.get() + " was " + avg );

        context.write( key, new DoubleWritable( avg ) );
    }

}
