/*
 * Copyright 2010-2013 10gen Inc.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.util.Map.Entry;

/**
 * Tool for simplifying the setup and usage of Mongo Hadoop jobs using the Tool / Configured interfaces for use w/ a ToolRunner Primarily
 * useful in cases of XML Config files.
 *
 * Main will be a necessary method to run the job - suggested implementation template:
 * <p></p>
 * 
 * <pre>
 * public static void main(String[] args) throws Exception {
 *     System.exit(ToolRunner.run(new &lt;YourClass&gt;(), args));
 * }
 * </pre>
 * 
 * @author Brendan W. McAdams <brendan@10gen.com>
 */
public class MongoTool extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MongoTool.class);

    /**
     * Defines the name of the job on the cluster. Left non-final to allow tweaking with serial #s, etc.  Defaults to the
     * getClass().getSimpleName()
     */
    private String jobName = getClass().getSimpleName();

    public String getJobName() {
        return jobName;
    }

    public void setJobName(final String name) {
        jobName = name;
    }

    @SuppressWarnings("deprecation")
    public int run(final String[] args) throws Exception {
        /**
         * ToolRunner will configure/process/setup the config
         * so we need to grab the class level one
         * This will be inited with any loaded xml files or -D prop=value params
         */
        final Configuration conf = getConf();

        LOG.info(String.format("Created a conf: '%s' on {%s} as job named '%s'", conf, this.getClass(), getJobName()));

        if (LOG.isTraceEnabled()) {
            for (final Entry<String, String> entry : conf) {
                LOG.trace(String.format("%s=%s\n", entry.getKey(), entry.getValue()));
            }
        }

        final Job job = new Job(conf, getJobName());
        /**
         * Any arguments specified with -D <property>=<value>
         * on the CLI will be picked up and set here
         * They override any XML level values
         * Note that -D<space> is important - no space will
         * not work as it gets picked up by Java itself
         */
        // TODO - Do we need to set job name somehow more specifically?
        // This may or may not be correct/sane
        job.setJarByClass(this.getClass());
        final Class mapper = MongoConfigUtil.getMapper(conf);

        LOG.debug("Mapper Class: " + mapper);
        LOG.debug("Input URI: " + MongoConfigUtil.getInputURI(conf));
        job.setMapperClass(mapper);
        Class<? extends Reducer> combiner = MongoConfigUtil.getCombiner(conf);
        if (combiner != null) {
            job.setCombinerClass(combiner);
        }
        job.setReducerClass(MongoConfigUtil.getReducer(conf));

        job.setOutputFormatClass(MongoConfigUtil.getOutputFormat(conf));
        job.setOutputKeyClass(MongoConfigUtil.getOutputKey(conf));
        job.setOutputValueClass(MongoConfigUtil.getOutputValue(conf));
        job.setInputFormatClass(MongoConfigUtil.getInputFormat(conf));
        Class mapOutputKeyClass = MongoConfigUtil.getMapperOutputKey(conf);
        Class mapOutputValueClass = MongoConfigUtil.getMapperOutputValue(conf);

        if (mapOutputKeyClass != null) {
            job.setMapOutputKeyClass(mapOutputKeyClass);
        }
        if (mapOutputValueClass != null) {
            job.setMapOutputValueClass(mapOutputValueClass);
        }

        /**
         * Determines if the job will run verbosely e.g. print debug output
         * Only works with foreground jobs
         */
        final boolean verbose = MongoConfigUtil.isJobVerbose(conf);
        /**
         * Run job in foreground aka wait for completion or background?
         */
        final boolean background = MongoConfigUtil.isJobBackground(conf);
        try {
            if (background) {
                LOG.info("Setting up and running MapReduce job in background.");
                job.submit();
                return 0;
            } else {
                LOG.info("Setting up and running MapReduce job in foreground, will wait for results.  {Verbose? "
                         + verbose + "}");
                return job.waitForCompletion(true) ? 0 : 1;
            }
        } catch (final Exception e) {
            LOG.error("Exception while executing job... ", e);
            return 1;
        }
    }
}
