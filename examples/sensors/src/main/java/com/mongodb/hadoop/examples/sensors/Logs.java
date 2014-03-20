package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import java.net.UnknownHostException;

public class Logs extends MongoTool {

    private static final Log LOG = LogFactory.getLog(Logs.class);

    public Logs() throws UnknownHostException {
        /*
       "mongo.input.uri=mongodb://localhost:27017/mongo_hadoop.logs",
                   "mongo.job.input.format=com.mongodb.hadoop.MongoInputFormat",
                   "mongo.job.mapper=com.mongodb.hadoop.examples.sensors.LogMapper",
                   "mongo.job.reducer=com.mongodb.hadoop.examples.sensors.LogReducer",
                   "mapreduce.combiner.class=com.mongodb.hadoop.examples.sensors.LogCombiner",
                   "mongo.job.combiner=com.mongodb.hadoop.examples.sensors.LogCombiner",
                   ,
       
                   "mongo.job.output.key=org.apache.hadoop.io.Text",
                   "mongo.job.output.value=org.apache.hadoop.io.IntWritable",
       
                   "mongo.output.uri=mongodb://localhost:27017/mongo_hadoop.logs_aggregate",
                   "mongo.job.output.format=com.mongodb.hadoop.MongoOutputFormat"       
         */
        System.out.println("************  Sensor Data Stage 2");
        Configuration conf = new Configuration();
        MongoConfig config = new MongoConfig(conf);
        setConf(conf);

        config.setInputFormat(MongoInputFormat.class);
        config.setInputURI("mongodb://localhost:27017/mongo_hadoop.logs");
        config.setOutputFormat(MongoOutputFormat.class);
        config.setOutputURI("mongodb://localhost:27017/mongo_hadoop.logs_aggregate");

        config.setMapper(LogMapper.class);
        config.setReducer(LogReducer.class);
        config.setCombiner(LogCombiner.class);
        
        config.setOutputKey(Text.class);
        config.setOutputValue(IntWritable.class);
    }

    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new Logs(), pArgs));
    }
}