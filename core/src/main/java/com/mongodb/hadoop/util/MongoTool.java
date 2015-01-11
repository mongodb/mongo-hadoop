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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Map.Entry;

/**
 * Tool for simplifying the setup and usage of Mongo Hadoop jobs using the Tool / Configured interfaces for use w/ a ToolRunner Primarily
 * useful in cases of XML Config files. <p/> Main will be a necessary method to run the job - suggested implementation template: <p></p>
 * <p/>
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
    private static final Boolean MAPRED_V1;

    static {
        boolean mapred;
        try {
            FileSystem.class.getDeclaredField("DEFAULT_FS");
            mapred = false;
        } catch (NoSuchFieldException e) {
            mapred = true;
        }
        
        MAPRED_V1 = mapred;
    }

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

    public int run(final String[] args) throws Exception {
        /**
         * ToolRunner will configure/process/setup the config
         * so we need to grab the class level one
         * This will be inited with any loaded xml files or -D prop=value params
         */
        final Configuration conf = getConf();

        LOG.info(String.format("Created a conf: '%s' on {%s} as job named '%s'", conf, getClass(), getJobName()));

        if (LOG.isTraceEnabled()) {
            for (final Entry<String, String> entry : conf) {
                LOG.trace(String.format("%s=%s\n", entry.getKey(), entry.getValue()));
            }
        }

        if (isMapRedV1()) {
            return runMapredJob(conf);
        } else {
            return runMapReduceJob(conf);
        }
    }

    public static boolean isMapRedV1() {
        return MAPRED_V1;
    }

    private int runMapredJob(final Configuration conf) {
        final JobConf job = new JobConf(conf, getClass());
        /**
         * Any arguments specified with -D <property>=<value>
         * on the CLI will be picked up and set here
         * They override any XML level values
         * Note that -D<space> is important - no space will
         * not work as it gets picked up by Java itself
         */
        // TODO - Do we need to set job name somehow more specifically?
        // This may or may not be correct/sane
        job.setJarByClass(getClass());
        final Class<? extends org.apache.hadoop.mapred.Mapper> mapper = MapredMongoConfigUtil.getMapper(conf);

        LOG.debug("Mapper Class: " + mapper);
        LOG.debug("Input URI: " + conf.get(MapredMongoConfigUtil.INPUT_URI));
        job.setMapperClass(mapper);
        Class<? extends org.apache.hadoop.mapred.Reducer> combiner = MapredMongoConfigUtil.getCombiner(conf);
        if (combiner != null) {
            job.setCombinerClass(combiner);
        }
        job.setReducerClass(MapredMongoConfigUtil.getReducer(conf));

        job.setOutputFormat(MapredMongoConfigUtil.getOutputFormat(conf));
        job.setOutputKeyClass(MapredMongoConfigUtil.getOutputKey(conf));
        job.setOutputValueClass(MapredMongoConfigUtil.getOutputValue(conf));
        job.setInputFormat(MapredMongoConfigUtil.getInputFormat(conf));
        Class mapOutputKeyClass = MapredMongoConfigUtil.getMapperOutputKey(conf);
        Class mapOutputValueClass = MapredMongoConfigUtil.getMapperOutputValue(conf);

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
        final boolean verbose = MapredMongoConfigUtil.isJobVerbose(conf);
        /**
         * Run job in foreground aka wait for completion or background?
         */
        final boolean background = MapredMongoConfigUtil.isJobBackground(conf);
        try {
            RunningJob runningJob = JobClient.runJob(job);
            if (background) {
                LOG.info("Setting up and running MapReduce job in background.");
                return 0;
            } else {
                LOG.info("Setting up and running MapReduce job in foreground, will wait for results.  {Verbose? "
                         + verbose + "}");
                runningJob.waitForCompletion();
                return 0;
            }
        } catch (final Exception e) {
            LOG.error("Exception while executing job... ", e);
            return 1;
        }

    }

    private int runMapReduceJob(final Configuration conf) throws IOException {
        final Job job = Job.getInstance(conf, getJobName());
        /**
         * Any arguments specified with -D <property>=<value>
         * on the CLI will be picked up and set here
         * They override any XML level values
         * Note that -D<space> is important - no space will
         * not work as it gets picked up by Java itself
         */
        // TODO - Do we need to set job name somehow more specifically?
        // This may or may not be correct/sane
        job.setJarByClass(getClass());
        final Class<? extends Mapper> mapper = MongoConfigUtil.getMapper(conf);

        LOG.debug("Mapper Class: " + mapper);
        LOG.debug("Input URI: " + conf.get(MongoConfigUtil.INPUT_URI));
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
