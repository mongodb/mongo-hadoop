package com.mongodb.hadoop.examples.sensors;
import org.bson.*;
import org.bson.types.ObjectId;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.util.*;
import com.mongodb.hadoop.io.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
import java.util.*;

public class LogMapper extends Mapper<Object, BSONObject, Text, IntWritable>{

    /*
        {
          "_id": ObjectId("51b792d381c3e67b0a18d678"),
          "d_id": ObjectId("51b792d381c3e67b0a18d4a1"),
          "v": 3328.5895416489802,
          "timestamp": ISODate("2013-05-18T13:11:38.709-0400"),
          "loc": [
            -175.13,
            51.658
          ]
        }
    */

	@Override
	public void map(Object key, BSONObject val, final Context context) throws IOException, InterruptedException{
        context.write(new Text(((ObjectId)val.get("d_id")).toString()), new IntWritable(1));
    }

}

