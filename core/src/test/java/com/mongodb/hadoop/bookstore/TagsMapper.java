package com.mongodb.hadoop.bookstore;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

import java.io.IOException;

public class TagsMapper extends Mapper<NullWritable, BSONObject, Text, BSONWritable> {
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
}
