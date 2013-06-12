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
import org.apache.commons.logging.*;
import java.io.*;
import java.util.*;

public class LogReducer extends Reducer<Text, IntWritable, NullWritable, MongoUpdateWritable> {

    private static final Log LOG = LogFactory.getLog( LogReducer.class );

    @Override
    public void reduce( final Text pKey,
                        final Iterable<IntWritable> pValues,
                        final Context pContext )
            throws IOException, InterruptedException{
        
        int count = 0;
        for(IntWritable val : pValues){
            count += val.get();
        }

        BasicBSONObject query = new BasicBSONObject("devices", new ObjectId(pKey.toString()));
        BasicBSONObject update = new BasicBSONObject("$inc", new BasicBSONObject("logs_count", count));
        LOG.info("query: " + query);
        LOG.info("update: " + update);
        pContext.write(null, new MongoUpdateWritable(query, update, true, false));
    }

}
