package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class DeviceReducer extends Reducer<Text, Text, NullWritable, MongoUpdateWritable>
    implements org.apache.hadoop.mapred.Reducer<Text, Text, NullWritable, MongoUpdateWritable> {

    private MongoUpdateWritable reduceResult;

    public DeviceReducer() {
        super();
        reduceResult = new MongoUpdateWritable();
    }

    @Override
    public void reduce(final Text pKey, final Iterable<Text> pValues, final Context pContext) throws IOException, InterruptedException {
        BasicBSONObject query = new BasicBSONObject("_id", pKey.toString());
        ArrayList<ObjectId> devices = new ArrayList<ObjectId>();
        for (Text val : pValues) {
            devices.add(new ObjectId(val.toString()));
        }

        BasicBSONObject update = new BasicBSONObject("$pushAll", new BasicBSONObject("devices", devices));
        reduceResult.setQuery(query);
        reduceResult.setModifiers(update);
        pContext.write(null, reduceResult);
    }

    @Override
    public void reduce(final Text key, final Iterator<Text> values, final OutputCollector<NullWritable, MongoUpdateWritable> output,
                       final Reporter reporter) throws IOException {
        BasicBSONObject query = new BasicBSONObject("_id", key.toString());
        ArrayList<ObjectId> devices = new ArrayList<ObjectId>();
        while (values.hasNext()) {
            Text val = values.next();
            devices.add(new ObjectId(val.toString()));
        }

        BasicBSONObject update = new BasicBSONObject("$pushAll", new BasicBSONObject("devices", devices));
        reduceResult.setQuery(query);
        reduceResult.setModifiers(update);
        output.collect(null, reduceResult);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf job) {
    }
}
