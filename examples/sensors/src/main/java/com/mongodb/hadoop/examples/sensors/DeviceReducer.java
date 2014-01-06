
package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.ArrayList;

public class DeviceReducer extends Reducer<Text, Text, NullWritable, MongoUpdateWritable> {

    @Override
    public void reduce(final Text pKey,
                       final Iterable<Text> pValues,
                       final Context pContext)
        throws IOException, InterruptedException {

        BasicBSONObject query = new BasicBSONObject("_id", pKey.toString());
        ArrayList<ObjectId> devices = new ArrayList<ObjectId>();
        for (Text val : pValues) {
            devices.add(new ObjectId(val.toString()));
        }

        BasicBSONObject update = new BasicBSONObject("$pushAll", new BasicBSONObject("devices", devices));
        pContext.write(null, new MongoUpdateWritable(query, update, true, false));
    }

}
