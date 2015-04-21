package com.mongodb.hadoop.bookstore;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

import java.io.IOException;

public class TagsMapper extends Mapper<Object, BSONObject, Text, BSONWritable>
  implements org.apache.hadoop.mapred.Mapper<Object, BSONWritable, Text,
  BSONWritable> {

    private BSONWritable writable;

    public TagsMapper() {
        super();
        writable = new BSONWritable();
    }

    @Override
    protected void map(final Object key, final BSONObject value, final Context
      context) throws IOException, InterruptedException {
        BasicBSONList tags = (BasicBSONList) value.get("tags");
        Text text = new Text();
        value.removeField("tags");
        for (Object tag : tags) {
            text.set((String) tag);
            writable.setDoc(value);
            context.write(text, writable);
        }
    }

    @Override
    public void map(final Object key, final BSONWritable value, final
    OutputCollector<Text, BSONWritable> output,
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
