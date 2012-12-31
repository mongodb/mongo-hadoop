package com.mongodb.hadoop.examples.enron;
import org.bson.*;
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

public class EnronMailMapper
	extends Mapper<NullWritable,BSONObject, BSONObject, IntWritable>{


	@Override
	public void map(NullWritable key, BSONObject val, final Context context)
        throws IOException, InterruptedException{
		if(val.containsKey("headers")){
			BSONObject headers = (BSONObject)val.get("headers");
			if(headers.containsKey("From") && headers.containsKey("To")){
				String from = (String)headers.get("From");
				String to = (String)headers.get("To");
				BSONObject outKey = BasicDBObjectBuilder.start()
										.add("f", from)
										.add("t", to)
										.get();
				context.write( new BSONWritable(outKey), new IntWritable(1) );
			}
		}
	}

}
