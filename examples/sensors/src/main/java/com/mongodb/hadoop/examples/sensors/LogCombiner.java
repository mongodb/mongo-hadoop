
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
import java.io.*;
import java.util.*;

public class LogCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Log LOG = LogFactory.getLog( LogCombiner.class );

    @Override
    public void reduce( final Text pKey,
                        final Iterable<IntWritable> pValues,
                        final Context pContext )
            throws IOException, InterruptedException{
        
        int count = 0;
        for(IntWritable val : pValues){
            count += val.get();
        }

        pContext.write(pKey, new IntWritable(count));
    }

}
