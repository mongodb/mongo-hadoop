package com.mongodb.spark.pickle;

import net.razorvine.pickle.Unpickler;
import org.bson.BSON;

public class RegisterConstructors {
    public void register() {
        Unpickler.registerConstructor("bson.binary", "Binary",
          new com.mongodb.spark.pickle.BinaryConstructor());
        Unpickler.registerConstructor("bson.code", "Code",
          new com.mongodb.spark.pickle.CodeConstructor());
        Unpickler.registerConstructor("bson.dbref", "DBRef",
          new com.mongodb.spark.pickle.DBRefConstructor());
        Unpickler.registerConstructor("bson.int64", "Int64",
          new com.mongodb.spark.pickle.Int64Constructor());
        Unpickler.registerConstructor("bson.max_key", "MaxKey",
          new com.mongodb.spark.pickle.MaxKeyConstructor());
        Unpickler.registerConstructor("bson.min_key", "MinKey",
          new com.mongodb.spark.pickle.MinKeyConstructor());
        Unpickler.registerConstructor("bson.timestamp", "Timestamp",
          new com.mongodb.spark.pickle.TimestampConstructor());
        Unpickler.registerConstructor("bson.regex", "Regex",
          new com.mongodb.spark.pickle.RegexConstructor());
        Unpickler.registerConstructor("bson.objectid", "ObjectId",
          new com.mongodb.spark.pickle.ObjectIdConstructor());

        BSON.addEncodingHook(
          java.util.GregorianCalendar.class,
          new CalendarTransformer());
    }
}
