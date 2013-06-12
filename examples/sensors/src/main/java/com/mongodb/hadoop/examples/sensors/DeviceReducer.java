
package com.mongodb.hadoop.examples.sensors;
import org.bson.*;
import org.bson.types.ObjectId;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.util.*;
import com.mongodb.hadoop.io.*;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
import java.util.*;

public class DeviceReducer extends Reducer<Text, Text, NullWritable, MongoUpdateWritable> {

    @Override
    public void reduce( final Text pKey,
                        final Iterable<Text> pValues,
                        final Context pContext )
            throws IOException, InterruptedException{
        
        BasicBSONObject query = new BasicBSONObject("_id", pKey.toString());
        ArrayList<ObjectId> devices = new ArrayList<ObjectId>();
        for(Text val : pValues){
            devices.add(new ObjectId(val.toString()));
        }

        BasicBSONObject update = new BasicBSONObject("$pushAll", new BasicBSONObject("devices", devices));
        pContext.write(null, new MongoUpdateWritable(query, update, true, false));
    }

}
