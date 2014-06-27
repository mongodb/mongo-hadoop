package com.mongodb.hadoop.examples.sensors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class LogCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
    implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Log LOG = LogFactory.getLog(LogCombiner.class);

    @Override
    public void reduce(final Text pKey, final Iterable<IntWritable> pValues, final Context pContext)
        throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable val : pValues) {
            count += val.get();
        }

        pContext.write(pKey, new IntWritable(count));
    }

    @Override
    public void reduce(final Text key, final Iterator<IntWritable> values, final OutputCollector<Text, IntWritable> output,
                       final Reporter reporter) throws IOException {
        int count = 0;
        while (values.hasNext()) {
            count += values.next().get();
        }

        output.collect(key, new IntWritable(count));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf job) {
    }
}
