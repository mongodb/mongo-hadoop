package com.mongodb.hadoop.streaming;

import com.mongodb.hadoop.io.*;
import com.mongodb.hadoop.streaming.io.MongoIdentifierResolver;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.streaming.*;
import org.apache.hadoop.streaming.io.IdentifierResolver;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.cli.*;

import java.io.IOException;

public class MongoStreamJob extends StreamJobPatch {
    @Override
    public int run(String[] args) throws Exception {
        try {
            Configuration conf = getConf();
            conf.setClass("stream.io.identifier.resolver.class", MongoIdentifierResolver.class, IdentifierResolver.class);
            log.info("Running");
            this.argv_ = args;
            log.info("Init");
            init();

            addOption("inputURI", "MongoDB URI for where to read input data from", 
                                  "inputURI", 1, true);
            addOption( "outputURI", "MongoDB URI for where to write output data to",
                       "outputURI", 1, true );
            log.info( "Process Args" );
            processArguments();
            log.info( "Args processed." );
            MongoConfigUtil.setInputURI( conf, _inputURI );
            MongoConfigUtil.setOutputURI( conf, _outputURI );
            setJobConf();
            jobConf_.setOutputKeyClass( BSONWritable.class );
            jobConf_.setOutputValueClass( BSONWritable.class );
            jobConf_.setOutputKeyComparatorClass( BSONComparator.class );
            log.info("Input Format: " + jobConf_.getInputFormat());
            log.info("Output Format: " + jobConf_.getOutputFormat());
            log.info("Key Class: " + jobConf_.getOutputKeyClass());

        } catch (Exception ex) {
            log.error("Error in streaming job", ex);
        }

        return super.submitAndMonitorJob();
    }

    @Override
    public int submitAndMonitorJob() throws IOException {
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MongoStreamJob(), args);
        if (exitCode != 0) {
            System.err.println("MongoDB Streaming Command Failed!");
            System.exit(exitCode);
        }
    }

    private static final Log log = LogFactory.getLog(MongoStreamJob.class);

}
