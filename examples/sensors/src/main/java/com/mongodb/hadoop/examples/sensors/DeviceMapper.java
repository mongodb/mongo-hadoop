package com.mongodb.hadoop.examples.sensors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

public class DeviceMapper extends Mapper<Object, BSONObject, Text, Text> {

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

}
