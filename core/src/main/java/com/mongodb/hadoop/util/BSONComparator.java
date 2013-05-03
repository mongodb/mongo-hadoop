package com.mongodb.hadoop.util;
import java.util.Comparator;
import org.bson.LazyBSONDecoder;
import org.bson.LazyBSONCallback;
import org.bson.BSONObject;
import org.apache.hadoop.io.RawComparator;

public class BSONComparator implements RawComparator<BSONObject> {

    private static final BSONComparator instance;

    static {
        instance = new BSONComparator();
    }

    public static BSONComparator getInstance(){
        return instance;
    }
    
    public int compare(BSONObject obj1, BSONObject obj2){
        //TODO make this more efficient. 
        //Shouldn't need to serialize the whole doc to a string just to compare two BSONObjects.
        return obj1.toString().compareTo( obj2.toString() );
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
        System.out.println("called compare!");
        LazyBSONDecoder dec = new LazyBSONDecoder();
        LazyBSONCallback cb = new LazyBSONCallback();
        dec.decode(b1, cb);
        BSONObject a = (BSONObject)cb.get();
        cb.reset();
        dec.decode(b2, cb);
        BSONObject b = (BSONObject)cb.get();
        System.out.println("about to call compare");
        return compare(a, b);
    }


}
