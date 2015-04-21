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

    private final IntWritable intw;
    private final MailPair mp;

    public EnronMailMapper() {
        super();
        intw = new IntWritable(1);
        mp = new MailPair();
    }

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
                    mp.setFrom((String) key);
                    mp.setTo(recip);
                    context.write(mp, intw);
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
                    mp.setFrom(from);
                    mp.setTo(recip);
                    output.collect(mp, intw);
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
