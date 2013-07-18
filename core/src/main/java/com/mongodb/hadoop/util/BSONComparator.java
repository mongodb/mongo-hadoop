/*
 * Copyright 2010-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
