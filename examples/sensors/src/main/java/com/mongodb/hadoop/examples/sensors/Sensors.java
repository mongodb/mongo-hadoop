package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Sensors extends MongoTool {

    public Sensors() {
        setJobName("Sensors Aggregation");
    }

    public static void main(final String[] pArgs) throws Exception {
        Configuration conf = new Configuration();
        System.exit(ToolRunner.run(conf, new Sensors(), pArgs));
    }

}


