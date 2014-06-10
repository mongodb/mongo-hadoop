package com.mongodb.hadoop.bookstore;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.ArrayList;

public class TagsReducer extends Reducer<Text, BSONWritable, NullWritable, MongoUpdateWritable> {
    @Override
    protected void reduce(final Text key, final Iterable<BSONWritable> values, final Context context)
        throws IOException, InterruptedException {

        BasicDBObject query = new BasicDBObject("_id", key.toString());
        ArrayList<BSONObject> books = new ArrayList<BSONObject>();
        for (BSONWritable val : values) {
            books.add(val.getDoc());
        }

        BasicBSONObject update = new BasicBSONObject("$set", new BasicBSONObject("books", books));
        context.write(null, new MongoUpdateWritable(query, update, true, false));
    }
}
