package com.mongodb.spark;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.spark.pickle.RegisterConstructors;
import com.mongodb.spark.pickle.RegisterPickles;

/**
 * InputFormat that attaches custom Picklers and IObjectConstructors for
 * reading and writing BSON types with PyMongo.
 */
public class PySparkMongoInputFormat extends MongoInputFormat {
    private static final RegisterPickles PICKLES = new RegisterPickles();
    private static final RegisterConstructors CONSTRUCTORS =
      new RegisterConstructors();

    static {
        PICKLES.register();
        CONSTRUCTORS.register();
    }
}
