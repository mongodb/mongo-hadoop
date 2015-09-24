package com.mongodb.hadoop.bookstore;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class TagsReducer extends Reducer<Text, BSONWritable, NullWritable, MongoUpdateWritable> 
    implements org.apache.hadoop.mapred.Reducer<Text, BSONWritable, NullWritable, MongoUpdateWritable> {

    private MongoUpdateWritable reduceResult;

    public TagsReducer() {
        super();
        reduceResult = new MongoUpdateWritable();
    }

    @Override
    protected void reduce(final Text key, final Iterable<BSONWritable> values, final Context context)
        throws IOException, InterruptedException {

        BasicDBObject query = new BasicDBObject("_id", key.toString());
        ArrayList<BSONObject> books = new ArrayList<BSONObject>();
        for (BSONWritable val : values) {
            books.add(val.getDoc());
        }

        BasicBSONObject update = new BasicBSONObject("$set", new BasicBSONObject("books", books));
        reduceResult.setQuery(query);
        reduceResult.setModifiers(update);
        context.write(null, reduceResult);
    }

    @Override
    public void reduce(final Text key, final Iterator<BSONWritable> values, final OutputCollector<NullWritable, MongoUpdateWritable> output,
                       final Reporter reporter) throws IOException {
        BasicDBObject query = new BasicDBObject("_id", key.toString());
        ArrayList<BSONObject> books = new ArrayList<BSONObject>();
        while (values.hasNext()) {
            books.add(values.next().getDoc());
        }

        BasicBSONObject update = new BasicBSONObject("$set", new BasicBSONObject("books", books));
        reduceResult.setQuery(query);
        reduceResult.setModifiers(update);
        output.collect(null, reduceResult);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf job) {
    }
}
