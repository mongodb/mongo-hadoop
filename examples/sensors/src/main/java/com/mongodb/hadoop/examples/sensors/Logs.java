package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import java.net.UnknownHostException;

public class Logs extends MongoTool {

    public Logs() throws UnknownHostException {
        Configuration conf = new Configuration();
        setConf(conf);
        boolean mrv1Job;
        try {
            FileSystem.class.getDeclaredField("DEFAULT_FS");
            mrv1Job = false;
        } catch (NoSuchFieldException e) {
            mrv1Job = true;
        }
        if (mrv1Job) {
            MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
            MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
        } else {
            MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
            MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
        }


        MongoConfigUtil.setInputURI(getConf(), "mongodb://localhost:27017/mongo_hadoop.logs");
        MongoConfigUtil.setOutputURI(getConf(), "mongodb://localhost:27017/mongo_hadoop.logs_aggregate");

        MongoConfigUtil.setMapper(getConf(), LogMapper.class);
        MongoConfigUtil.setReducer(getConf(), LogReducer.class);
        MongoConfigUtil.setCombiner(getConf(), LogCombiner.class);

        MongoConfigUtil.setOutputKey(getConf(), Text.class);
        MongoConfigUtil.setOutputValue(getConf(), IntWritable.class);
    }

    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new Logs(), pArgs));
    }
}