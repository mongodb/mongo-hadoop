package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

public class LogMapper extends Mapper<Object, BSONObject, Text, IntWritable> 
    implements org.apache.hadoop.mapred.Mapper<Object, BSONWritable, Text, IntWritable> {

    private final Text keyText;
    private final IntWritable valueInt;

    public LogMapper() {
        super();
        keyText = new Text();
        valueInt = new IntWritable(1);
    }

    @Override
    public void map(final Object key, final BSONObject val, final Context context) throws IOException, InterruptedException {
        keyText.set(val.get("d_id").toString());
        context.write(keyText, valueInt);
    }

    @Override
    public void map(final Object key, final BSONWritable value, final OutputCollector<Text, IntWritable> output, final Reporter reporter)
        throws IOException {
        keyText.set(value.getDoc().get("d_id").toString());
        output.collect(keyText, valueInt);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf job) {
    }
}

