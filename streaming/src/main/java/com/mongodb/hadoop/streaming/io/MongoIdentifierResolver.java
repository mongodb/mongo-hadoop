package com.mongodb.hadoop.streaming.io;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.streaming.io.IdentifierResolver;

public class MongoIdentifierResolver extends IdentifierResolver {
    public static final String MONGODB_ID = "mongodb";
    public static final String MONGO_ID = "mongo";
    public static final String BSON_ID = "bson";
    public static final String MONGODB_UPDATE = "mongoUpdate";

    @Override
    public void resolve(final String identifier) {
        if (identifier.equalsIgnoreCase(MONGODB_ID)
          || identifier.equalsIgnoreCase(MONGO_ID)
          || identifier.equalsIgnoreCase(BSON_ID)) {
            setInputWriterClass(MongoInputWriter.class);
            setOutputReaderClass(MongoOutputReader.class);
            setOutputKeyClass(BSONWritable.class);
            setOutputValueClass(BSONWritable.class);
        } else if (identifier.equalsIgnoreCase(MONGODB_UPDATE)) {
            setInputWriterClass(MongoUpdateInputWriter.class);
            setOutputReaderClass(MongoUpdateOutputReader.class);
            setOutputKeyClass(BSONWritable.class);
            setOutputValueClass(MongoUpdateWritable.class);
        } else {
            super.resolve(identifier);
        }
    }
}
