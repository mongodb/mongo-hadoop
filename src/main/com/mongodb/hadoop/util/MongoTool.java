// MongoTool.java
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

package com.mongodb.hadoop.util;

import java.util.Map.Entry;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

/**
 * Tool for simplifying the setup and usage of Mongo Hadoop jobs
 * using the Tool / Configured interfaces for use w/ a ToolRunner
 * Primarily useful in cases of XML Config files.
 * 
 * @author Brendan W. McAdams <brendan@10gen.com>
 */
public class MongoTool extends Configured implements Tool {
    private static final Log log = LogFactory.getLog( MongoTool.class );

    public int run( String[] args ) throws Exception{
        /**
         * ToolRunner will configure/process/setup the config
         * so we need to grab the classlevel one
         * This will be inited with any loaded xml files or -D prop=value params
         */
        final Configuration conf = getConf();

        log.info( "Created a conf: '" + conf + "' on {" + this.getClass() + "} as job named '" + _jobName + "'" );

        for ( final Entry<String, String> entry : conf ) {
            log.trace( String.format( "%s=%s\n", entry.getKey(), entry.getValue() ) );
        }

        final Job job = new Job( conf , _jobName );
        /**
         * Any arguments specified with -D <property>=<value>
         * on the CLI will be picked up and set here
         * They override any XML level values
         * Note that -D<space> is important - no space will
         * not work as it get spicked up by Java itself
         */
        // TODO - Do we need to set job name somehow more specifically?
        // This may or may not be correct/sane
        job.setJarByClass( this.getClass() );
        final Class mapper = MongoConfigUtil.getMapper( conf );

        log.info( "Mapper Class: " + mapper );
        job.setMapperClass( mapper );
        job.setCombinerClass( MongoConfigUtil.getCombiner( conf ) );
        job.setReducerClass( MongoConfigUtil.getReducer( conf ) );

        job.setOutputFormatClass( MongoConfigUtil.getOutputFormat( conf ) );
        job.setOutputKeyClass( MongoConfigUtil.getOutputKey( conf ) );
        job.setOutputValueClass( MongoConfigUtil.getOutputValue( conf ) );

        job.setInputFormatClass( MongoConfigUtil.getInputFormat( conf ) );

        /**
         * Determines if the job will run verbosely e.g. print debug output
         * Only works with foreground jobs
         */
        final boolean verbose = MongoConfigUtil.isJobVerbose( conf );
        /**
         * Run job in foreground aka wait for completion or background?
         */
        final boolean background = MongoConfigUtil.isJobBackground( conf );
        try {
            if ( background ) {
                log.info( "Setting up and running MapReduce job in background." );
                job.submit();
                return 0;
            }
            else {
                log.info( "Setting up and running MapReduce job in foreground, will wait for results.  {Verbose? " + verbose + "}" );
                return job.waitForCompletion( true ) ? 0 : 1;
            }
        }
        catch ( final Exception e ) {
            log.error( "Exception while executing job... ", e );
            return 1;
        }
    }

    /**
     * Main will be a necessary method to run the job - suggested implementation
     * template:
     * public static void main(String[] args) throws Exception {
     * int exitCode = ToolRunner.run(new <YourClass>(), args);
     * System.exit(exitCode);
     * }
     * 
     */

    /**
     * SET ME
     * Defines the name of the job on the cluster.
     * Left non-final to allow tweaking with serial #s, etc
     **/
    String _jobName = "<unnamed MongoTool job>";
}
