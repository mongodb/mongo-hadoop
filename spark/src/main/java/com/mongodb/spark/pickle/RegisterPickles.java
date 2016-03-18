package com.mongodb.spark.pickle;

import net.razorvine.pickle.Pickler;

public class RegisterPickles {
    private static final BSONPickler PICKLER = new BSONPickler();

    public void register() {
        Pickler.registerCustomPickler(org.bson.types.ObjectId.class, PICKLER);
        Pickler.registerCustomPickler(org.bson.types.Binary.class, PICKLER);
        Pickler.registerCustomPickler(org.bson.types.Code.class, PICKLER);
        Pickler.registerCustomPickler(org.bson.types.CodeWScope.class, PICKLER);
        Pickler.registerCustomPickler(
          org.bson.types.CodeWithScope.class, PICKLER);
        Pickler.registerCustomPickler(org.bson.types.MaxKey.class, PICKLER);
        Pickler.registerCustomPickler(org.bson.types.MinKey.class, PICKLER);
        Pickler.registerCustomPickler(
          org.bson.types.BSONTimestamp.class, PICKLER);
        Pickler.registerCustomPickler(com.mongodb.DBRef.class, PICKLER);
        Pickler.registerCustomPickler(java.util.regex.Pattern.class, PICKLER);
        Pickler.registerCustomPickler(java.util.Date.class, PICKLER);
    }
}
