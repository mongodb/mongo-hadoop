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
package com.mongodb.hadoop.examples;

// Mongo
import org.bson.*;
import com.mongodb.hadoop.util.*;

// Commons
import org.apache.commons.logging.*;

// Hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Java
import java.io.*;
import java.util.*;

/**
 * The treasury yield reducer.
 */
public class TreasuryYieldReducer
    extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>
{
    @Override
    public void reduce( final IntWritable pKey,
                        final Iterable<DoubleWritable> pValues,
                        final Context pContext )
        throws IOException, InterruptedException
    {
        int count = 0;
        double sum = 0;
        for ( final DoubleWritable value : pValues ) {
            LOG.debug( "Key: " + pKey + " Value: " + value );
            sum += value.get();
            count++;
        }

        final double avg = sum / count;

        LOG.debug( "Average 10 Year Treasury for " + pKey.get() + " was " + avg );

        pContext.write( pKey, new DoubleWritable( avg ) );
    }

    private static final Log LOG = LogFactory.getLog( TreasuryYieldReducer.class );

}

