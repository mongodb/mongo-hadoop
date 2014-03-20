package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import java.net.UnknownHostException;

public class Devices extends MongoTool {

    public Devices() throws UnknownHostException {
        Configuration conf = new Configuration();
        MongoConfig config = new MongoConfig(conf);
        setConf(conf);

        config.setInputFormat(MongoInputFormat.class);
        config.setInputURI("mongodb://localhost:27017/mongo_hadoop.devices");
        config.setOutputFormat(MongoOutputFormat.class);
        config.setOutputURI("mongodb://localhost:27017/mongo_hadoop.logs_aggregate");

        config.setMapper(DeviceMapper.class);
        config.setReducer(DeviceReducer.class);
        config.setMapperOutputKey(Text.class);
        config.setMapperOutputValue(Text.class);
        config.setOutputKey(IntWritable.class);
        config.setOutputValue(BSONWritable.class);

        new SensorDataGenerator().run();
    }

    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new Devices(), pArgs));
    }
}