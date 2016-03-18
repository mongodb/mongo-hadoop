package com.mongodb.spark;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.spark.pickle.RegisterConstructors;
import com.mongodb.spark.pickle.RegisterPickles;

public class PySparkMongoOutputFormat<K, V>
  extends MongoOutputFormat<K, V> {
    private static final RegisterPickles PICKLES = new RegisterPickles();
    private static final RegisterConstructors CONSTRUCTORS =
      new RegisterConstructors();

    static {
        PICKLES.register();
        CONSTRUCTORS.register();
    }
}
