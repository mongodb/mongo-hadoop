package com.mongodb.hadoop.examples.enron;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

public class EnronMailMapper extends Mapper<Object, BSONObject, MailPair, IntWritable>
    implements org.apache.hadoop.mapred.Mapper<Object, BSONWritable, MailPair, IntWritable> {
    @Override
    public void map(final Object key, final BSONObject val,
                    final Context context)
            throws IOException, InterruptedException {

        BSONObject headers = (BSONObject) val.get("headers");
        String to = (String) headers.get("To");
        if (null != to) {
            String[] recipients = to.split(",");
            for (final String recip1 : recipients) {
                String recip = recip1.trim();
                if (recip.length() > 0) {
                    context.write(new MailPair((String) key, recip),
                                  new IntWritable(1));
                }
            }
        }
    }

    @Override
    public void map(final Object key, final BSONWritable writable, final OutputCollector<MailPair, IntWritable> output,
                    final Reporter reporter) throws IOException {
        BSONObject headers = (BSONObject) writable.getDoc().get("headers");
        String to = (String) headers.get("To");
        String from = (String) headers.get("From");
        if (null != to) {
            String[] recipients = to.split(",");
            for (final String recip1 : recipients) {
                String recip = recip1.trim();
                if (recip.length() > 0) {
                    output.collect(new MailPair(from, recip),
                                   new IntWritable(1));
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf job) {
    }
}
