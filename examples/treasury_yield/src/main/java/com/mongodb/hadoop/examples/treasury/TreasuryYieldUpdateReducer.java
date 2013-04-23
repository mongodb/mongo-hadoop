
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
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.util.*;
import com.mongodb.hadoop.io.MongoUpdateWritable;

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
public class TreasuryYieldUpdateReducer
        extends Reducer<IntWritable, DoubleWritable, NullWritable, MongoUpdateWritable> {
    @Override
    public void reduce( final IntWritable pKey,
                        final Iterable<DoubleWritable> pValues,
                        final Context pContext )
            throws IOException, InterruptedException{
        int count = 0;
        double sum = 0;
        for ( final DoubleWritable value : pValues ){
            sum += value.get();
            count++;
            //LOG.info( "Key: " + pKey + " Value: " + value + " count: " + count + " sum: " + sum );
        }

        final double avg = sum / count;

        LOG.debug( "Average 10 Year Treasury for " + pKey.get() + " was " + avg );

        BasicBSONObject output = new BasicBSONObject();
        output.put("count", count);
        output.put("avg", avg);
        output.put("sum", sum);
        BasicBSONObject query = new BasicBSONObject("_id", pKey.get());
        BasicBSONObject modifiers = new BasicBSONObject();
        modifiers.put("$set", BasicDBObjectBuilder.start().
                                  add( "count", count ).
                                  add( "avg", avg ).
                                  add( "sum", sum ).get());

        modifiers.put("$push", new BasicBSONObject("calculatedAt", new Date()));
        modifiers.put("$inc", new BasicBSONObject("numCalculations", 1));
        pContext.write( null, new MongoUpdateWritable(query, modifiers, true, false) );
    }

    private static final Log LOG = LogFactory.getLog( TreasuryYieldReducer.class );

}

