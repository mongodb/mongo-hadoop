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
package com.mongodb.hadoop.examples.ufos;

// Mongo

import org.bson.*;
import org.bson.types.ObjectId;
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
 * The ufo sightings reducer.
 */
public class UfoSightingsReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce( final Text location,
                        final Iterable<IntWritable> sightings,
                        final Context pContext )
            throws IOException, InterruptedException{
        int count = 0;
        for ( final IntWritable v: sightings){
            LOG.debug( "Location: " + location + " Value: " + v );
            count += v.get();
        }

        pContext.write( location, new IntWritable( count ) );
    }

    private static final Log LOG = LogFactory.getLog( UfoSightingsReducer.class );

}

