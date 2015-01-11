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

    /*

        {
          "_id": ObjectId("51b792d381c3e67b0a18d0de"),
          "name": "BSGORNaN",
          "type": "temp",
          "owner": "Qs7GqRDcn7",
          "model": 11,
          "created_at": ISODate("2006-07-09T06:56:58.448-0400")
        }
    */

    @Override
    public void map(final Object key, final BSONObject val, final Context context) throws IOException, InterruptedException {
        String keyOut = (String) val.get("owner") + " " + (String) val.get("type");
        context.write(new Text(keyOut), new Text(val.get("_id").toString()));
    }

    @Override
    public void map(final Object key, final BSONWritable value, final OutputCollector<Text, Text> output,
                    final Reporter reporter) throws IOException {
        BSONObject val = value.getDoc();
        
        String keyOut = (String) val.get("owner") + " " + (String) val.get("type");
        output.collect(new Text(keyOut), new Text(val.get("_id").toString()));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf job) {
    }
}
