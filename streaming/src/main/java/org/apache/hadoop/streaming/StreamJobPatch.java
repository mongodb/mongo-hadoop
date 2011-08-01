// Monkey Patch time!
package org.apache.hadoop.streaming;

import org.apache.commons.cli.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

public class StreamJobPatch extends StreamJob {
    
    protected void processArguments() throws Exception {
        log.info("Setup Options'");
        setupOptions();
        log.info("PreProcess Args");
        preProcessArgs();
        log.info("Parse Options");
        parseOpts();
        log.info("Add InputSpecs");
        inputSpecs_.add("file://");
        log.info("Setup output_");
        output_ = "file:///tmp";
        log.info("Post Process Args");
        postProcessArgs();
    }

    protected void addOption(String name, String desc, 
                              String argName, int max, boolean required){
        _options.addOption(OptionBuilder
                 .withArgName(argName)
                 .hasArgs(max)
                 .withDescription(desc)
                 .isRequired(required)
                 .create(name));
    }

    protected void addBoolOption(String name, String desc){
        _options.addOption(OptionBuilder.withDescription(desc).create(name));
    }

    protected void parseOpts() {
        for ( String arg : argv_ ) {
            log.info("Arg: '" + arg + "'");
        }
        CommandLine cmdLine = null; 
        try{
          cmdLine = _parser.parse(_options, argv_, false);
        } catch(Exception oe) {
          LOG.error(oe.getMessage());
          exitUsage(argv_.length > 0 && "-info".equals(argv_[0]));
        }
        if (cmdLine != null){
            _inputURI =  cmdLine.getOptionValue("inputURI"); 
            _outputURI =  cmdLine.getOptionValue("outputURI"); 
            verbose_ =  cmdLine.hasOption("verbose");
            detailedUsage_ = cmdLine.hasOption("info");
            debug_ = cmdLine.hasOption("debug")? debug_ + 1 : debug_;
            mapCmd_ = cmdLine.getOptionValue("mapper"); 
            comCmd_ = cmdLine.getOptionValue("combiner"); 
            redCmd_ = cmdLine.getOptionValue("reducer"); 
      
                     
            String fsName = cmdLine.getOptionValue("dfs");
            if (null != fsName){
                LOG.warn("-dfs option is deprecated, please use -fs instead.");
                config_.set("fs.default.name", fsName);
            }
      
            inputFormatSpec_ = "com.mongodb.hadoop.mapred.MongoInputFormat";
            outputFormatSpec_ = "com.mongodb.hadoop.mapred.MongoOutputFormat";
            additionalConfSpec_ = cmdLine.getOptionValue("additionalconfspec"); 
            numReduceTasksSpec_ = cmdLine.getOptionValue("numReduceTasks"); 
            partitionerSpec_ = cmdLine.getOptionValue("partitioner");
            mapDebugSpec_ = cmdLine.getOptionValue("mapdebug");    
            reduceDebugSpec_ = cmdLine.getOptionValue("reducedebug");
            ioSpec_ = "bson";
      
            String[] car = cmdLine.getOptionValues("cacheArchive"); 
            if (null != car && car.length > 0){
                LOG.warn("-cacheArchive option is deprecated, please use -archives instead.");
                for(String s : car){
                    cacheArchives = (cacheArchives == null)?s :cacheArchives + "," + s;  
                }
            }

            String[] caf = cmdLine.getOptionValues("cacheFile"); 
            if (null != caf && caf.length > 0){
                LOG.warn("-cacheFile option is deprecated, please use -files instead.");
                for(String s : caf){
                    cacheFiles = (cacheFiles == null)?s :cacheFiles + "," + s;  
                }
            }
      
            String[] jobconf = cmdLine.getOptionValues("jobconf"); 
            if (null != jobconf && jobconf.length > 0){
                LOG.warn("-jobconf option is deprecated, please use -D instead.");
                for(String s : jobconf){
                    String[] parts = s.split("=", 2);
                    config_.set(parts[0], parts[1]);
                }
            }
      
            String[] cmd = cmdLine.getOptionValues("cmdenv"); 
            if (null != cmd && cmd.length > 0){
                for(String s : cmd) {
                  if (addTaskEnvironment_.length() > 0) {
                    addTaskEnvironment_ += " ";
                  }
                  addTaskEnvironment_ += s;
                }
            }
        } else {
          exitUsage(argv_.length > 0 && "-info".equals(argv_[0]));
        }

    }
    private void setupOptions(){
    
        addOption("inputURI", "MongoDB URI for where to read input data from", 
                              "inputURI", 1, true);
        addOption("outputURI", "MongoDB URI for where to write output data to", 
                               "outputURI", 1, true);
        addOption("mapper", "The streaming command to run", "cmd", 1, false);
        addOption("combiner", "The streaming command to run", "cmd", 1, false);
        // reducer could be NONE 
        addOption("reducer", "The streaming command to run", "cmd", 1, false); 
        addOption("jt", "Optional. Override JobTracker configuration", "<h:p>|local", 1, false);
        addOption("additionalconfspec", "Optional.", "spec", 1, false);
        addOption("inputformat", "Optional.", "spec", 1, false);
        addOption("outputformat", "Optional.", "spec", 1, false);
        addOption("partitioner", "Optional.", "spec", 1, false);
        addOption("numReduceTasks", "Optional.", "spec",1, false );
        addOption("mapdebug", "Optional.", "spec", 1, false);
        addOption("reducedebug", "Optional", "spec",1, false);
        addOption("jobconf", "(n=v) Optional. Add or override a JobConf property.", "spec", 1, false);
        addOption("cmdenv", "(n=v) Pass env.var to streaming commands.", "spec", 1, false);
        addOption("cacheFile", "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
        addOption("cacheArchive", "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
        addOption("io", "Optional.", "spec", 1, false);
        
        // boolean properties
        
        addBoolOption("verbose", "print verbose output"); 
        addBoolOption("info", "print verbose output"); 
        addBoolOption("help", "print this help message"); 
        addBoolOption("debug", "print debug output"); 
    }

    protected String _inputURI;
    protected String _outputURI;
    protected CommandLineParser _parser = new BasicParser(); 
    protected Options _options = new Options();
    private static final Log log = LogFactory.getLog(StreamJobPatch.class);
}


