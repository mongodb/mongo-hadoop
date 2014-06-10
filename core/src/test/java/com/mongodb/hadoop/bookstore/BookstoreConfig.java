package com.mongodb.hadoop.bookstore;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

public class BookstoreConfig extends MongoTool {
    public BookstoreConfig() {
        this(new Configuration());
    }

    public BookstoreConfig(final Configuration configuration) {
        MongoConfig config = new MongoConfig(configuration);
        setConf(configuration);

        config.setInputFormat(MongoInputFormat.class);

        config.setMapper(TagsMapper.class);
        config.setMapperOutputKey(Text.class);
        config.setMapperOutputValue(BSONWritable.class);

        config.setReducer(TagsReducer.class);
        config.setOutputKey(Text.class);
        config.setOutputValue(MongoUpdateWritable.class);
        config.setOutputFormat(MongoOutputFormat.class);
    }
    
    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new BookstoreConfig(), pArgs));
    }
}
