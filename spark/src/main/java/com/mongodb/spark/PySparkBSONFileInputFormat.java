package com.mongodb.spark;

import com.mongodb.hadoop.BSONFileInputFormat;
import com.mongodb.spark.pickle.RegisterConstructors;
import com.mongodb.spark.pickle.RegisterPickles;

public class PySparkBSONFileInputFormat extends BSONFileInputFormat {
    private static final RegisterPickles PICKLES = new RegisterPickles();
    private static final RegisterConstructors CONSTRUCTORS =
      new RegisterConstructors();

    static {
        PICKLES.register();
        CONSTRUCTORS.register();
    }
}
