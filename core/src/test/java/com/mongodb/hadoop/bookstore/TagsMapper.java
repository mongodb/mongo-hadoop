package com.mongodb.hadoop.bookstore;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

import java.io.IOException;

public class TagsMapper extends Mapper<NullWritable, BSONObject, Text, BSONWritable> 
    implements org.apache.hadoop.mapred.Mapper<NullWritable, BSONWritable, Text, BSONWritable> {

    @Override
    protected void map(final NullWritable key, final BSONObject value, final Context context) throws IOException, InterruptedException {
        BasicBSONList tags = (BasicBSONList) value.get("tags");
        Text text = new Text();
        value.removeField("tags");
        for (Object tag : tags) {
            text.set((String) tag);
            context.write(text, new BSONWritable(value));
        }
    }

    @Override
    public void map(final NullWritable key, final BSONWritable value, final OutputCollector<Text, BSONWritable> output,
                    final Reporter reporter) throws IOException {
        BasicBSONList tags = (BasicBSONList) value.getDoc().get("tags");
        Text text = new Text();
        value.getDoc().removeField("tags");
        for (Object tag : tags) {
            text.set((String) tag);
            output.collect(text, value);
        }
    }

    @Override
    public void configure(final JobConf job) {
        
    }

    @Override
    public void close() throws IOException {
    }
}
