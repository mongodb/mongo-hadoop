package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import java.net.UnknownHostException;

public class Devices extends MongoTool {

    public Devices() throws UnknownHostException {
        setConf(new Configuration());
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
        
        MongoConfigUtil.setInputURI(getConf(), "mongodb://localhost:27017/mongo_hadoop.devices");
        MongoConfigUtil.setOutputURI(getConf(), "mongodb://localhost:27017/mongo_hadoop.logs_aggregate");

        MongoConfigUtil.setMapper(getConf(), DeviceMapper.class);
        MongoConfigUtil.setReducer(getConf(), DeviceReducer.class);
        MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
        MongoConfigUtil.setMapperOutputValue(getConf(), Text.class);
        MongoConfigUtil.setOutputKey(getConf(), IntWritable.class);
        MongoConfigUtil.setOutputValue(getConf(), BSONWritable.class);
        
        new SensorDataGenerator().run();
    }

    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new Devices(), pArgs));
    }
}