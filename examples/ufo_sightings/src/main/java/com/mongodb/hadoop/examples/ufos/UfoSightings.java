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
import com.mongodb.hadoop.util.*;
import com.mongodb.hadoop.*;

// Hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapreduce.*;

/**
 * The ufo sightings xml config object.
 */

public class UfoSightings extends Configured implements Tool {
    private static final Log log = LogFactory.getLog( UfoSightings.class );

    public int run( String[] args ) throws Exception{
        final Configuration conf = getConf();

        final Job job = new Job( conf, "ufo-sightings" );
        final Class mapper = UfoSightingsMapper.class;
        job.setMapperClass( mapper );
        final Class reducer = UfoSightingsReducer.class;

        job.setReducerClass(UfoSightingsReducer.class);
        job.setOutputFormatClass(com.mongodb.hadoop.MongoOutputFormat.class);
        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.IntWritable.class);
        job.setInputFormatClass(com.mongodb.hadoop.MongoInputFormat.class);

        final boolean verbose = true;
        final boolean background = false;
        try {
            if ( background ){
                log.info( "Setting up and running MapReduce job in background." );
                job.submit();
                return 0;
            }
            else{
                log.info( "Setting up and running MapReduce job in foreground, will wait for results.  {Verbose? "
                          + verbose + "}" );
                return job.waitForCompletion( true ) ? 0 : 1;
            }
        } catch ( final Exception e ) {
            log.error( "Exception while executing job... ", e );
            return 1;
        }
    }

    public static void main( final String[] pArgs ) throws Exception{
        System.exit( ToolRunner.run( new UfoSightings(), pArgs ) );
    }
}

