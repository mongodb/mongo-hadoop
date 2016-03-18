package com.mongodb.spark;

import com.mongodb.hadoop.BSONFileOutputFormat;
import com.mongodb.spark.pickle.RegisterConstructors;
import com.mongodb.spark.pickle.RegisterPickles;

public class PySparkBSONFileOutputFormat<K, V>
  extends BSONFileOutputFormat<K, V> {
    private static final RegisterPickles PICKLES = new RegisterPickles();
    private static final RegisterConstructors CONSTRUCTORS =
      new RegisterConstructors();

    static {
        PICKLES.register();
        CONSTRUCTORS.register();
    }
}
