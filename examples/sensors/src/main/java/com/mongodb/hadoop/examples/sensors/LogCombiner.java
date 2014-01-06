
package com.mongodb.hadoop.examples.sensors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Log LOG = LogFactory.getLog(LogCombiner.class);

    @Override
    public void reduce(final Text pKey,
                       final Iterable<IntWritable> pValues,
                       final Context pContext)
        throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable val : pValues) {
            count += val.get();
        }

        pContext.write(pKey, new IntWritable(count));
    }

}
