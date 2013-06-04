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
	extends Mapper<NullWritable,BSONObject, MailPair, IntWritable>{

    private static final Log LOG = LogFactory.getLog( EnronMailMapper.class );


	@Override
	public void map(NullWritable key, BSONObject val, final Context context)
        throws IOException, InterruptedException{
		if(val.containsKey("headers")){
			BSONObject headers = (BSONObject)val.get("headers");
			if(headers.containsKey("From") && headers.containsKey("To")){
				String from = (String)headers.get("From");
				String to = (String)headers.get("To");
                String[] recips = to.split(",");
                for(int i=0;i<recips.length;i++){
                    String recip = recips[i].trim();
                    if(recip.length() > 0){
                        context.write(new MailPair(from, recip), new IntWritable(1));
                    }
                }
			}
		}
	}

}
