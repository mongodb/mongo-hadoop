package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

public class DeviceMapper extends Mapper<Object, BSONObject, Text, Text>
    implements org.apache.hadoop.mapred.Mapper<Object, BSONWritable, Text, Text> {

    private final Text keyText;
    private final Text valueText;

    public DeviceMapper() {
        super();
        keyText = new Text();
        valueText = new Text();
    }

    @Override
    public void map(final Object key, final BSONObject val, final Context context) throws IOException, InterruptedException {
        String keyOut = (String) val.get("owner") + " " + (String) val.get("type");
        keyText.set(keyOut);
        valueText.set(val.get("_id").toString());
        context.write(keyText, valueText);
    }

    @Override
    public void map(final Object key, final BSONWritable value, final OutputCollector<Text, Text> output,
                    final Reporter reporter) throws IOException {
        BSONObject val = value.getDoc();
        
        String keyOut = (String) val.get("owner") + " " + (String) val.get("type");
        keyText.set(keyOut);
        valueText.set(val.get("_id").toString());
        output.collect(keyText, valueText);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf job) {
    }
}
