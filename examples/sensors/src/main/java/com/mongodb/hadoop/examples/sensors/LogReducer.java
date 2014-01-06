package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.io.IOException;

public class LogReducer extends Reducer<Text, IntWritable, NullWritable, MongoUpdateWritable> {

    private static final Log LOG = LogFactory.getLog(LogReducer.class);

    @Override
    public void reduce(final Text pKey,
                       final Iterable<IntWritable> pValues,
                       final Context pContext)
        throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable val : pValues) {
            count += val.get();
        }

        BasicBSONObject query = new BasicBSONObject("devices", new ObjectId(pKey.toString()));
        BasicBSONObject update = new BasicBSONObject("$inc", new BasicBSONObject("logs_count", count));
        LOG.info("query: " + query);
        LOG.info("update: " + update);
        pContext.write(null, new MongoUpdateWritable(query, update, true, false));
    }

}
