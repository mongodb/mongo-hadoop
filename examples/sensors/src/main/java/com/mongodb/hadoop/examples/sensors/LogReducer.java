package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.Iterator;

public class LogReducer extends Reducer<Text, IntWritable, NullWritable, MongoUpdateWritable>
    implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, NullWritable, MongoUpdateWritable> {

    private static final Log LOG = LogFactory.getLog(LogReducer.class);

    @Override
    public void reduce(final Text pKey, final Iterable<IntWritable> pValues, final Context pContext)
        throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable val : pValues) {
            count += val.get();
        }

        BasicBSONObject query = new BasicBSONObject("devices", new ObjectId(pKey.toString()));
        BasicBSONObject update = new BasicBSONObject("$inc", new BasicBSONObject("logs_count", count));
        LOG.debug("query: " + query);
        LOG.debug("update: " + update);
        pContext.write(null, new MongoUpdateWritable(query, update, true, false));
    }

    @Override
    public void reduce(final Text key, final Iterator<IntWritable> values, final OutputCollector<NullWritable, MongoUpdateWritable> output,
                       final Reporter reporter) throws IOException {
        int count = 0;
        while (values.hasNext()) {
            count += values.next().get();
        }

        BasicBSONObject query = new BasicBSONObject("devices", new ObjectId(key.toString()));
        BasicBSONObject update = new BasicBSONObject("$inc", new BasicBSONObject("logs_count", count));
        LOG.debug("query: " + query);
        LOG.debug("update: " + update);
        output.collect(null, new MongoUpdateWritable(query, update, true, false));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf job) {
    }
}
