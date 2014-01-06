package com.mongodb.hadoop.examples.enron;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

public class EnronMailMapper extends Mapper<Object, BSONObject, MailPair, IntWritable> {
    @Override
    public void map(final Object key, final BSONObject val, final Context context)
        throws IOException, InterruptedException {

        if (val.containsField("headers")) {
            BSONObject headers = (BSONObject) val.get("headers");
            if (headers.containsField("From") && headers.containsField("To")) {
                String from = (String) headers.get("From");
                String to = (String) headers.get("To");
                String[] recipients = to.split(",");
                for (final String recip1 : recipients) {
                    String recip = recip1.trim();
                    if (recip.length() > 0) {
                        context.write(new MailPair(from, recip), new IntWritable(1));
                    }
                }
            }
        }
    }
}