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

package com.mongodb.hadoop.io;

import com.mongodb.hadoop.util.BSONComparator;
import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;

public class BSONWritableComparator extends WritableComparator {

    public BSONWritableComparator(){
        super( BSONWritable.class, true );
    }

    protected BSONWritableComparator( Class<? extends WritableComparable> keyClass ){
        super( keyClass, true );
    }

    protected BSONWritableComparator( Class<? extends WritableComparable> keyClass, boolean createInstances ){
        super( keyClass, createInstances );
    }


    public int compare( WritableComparable a, WritableComparable b ){
        if ( a instanceof BSONWritable && b instanceof BSONWritable ){
            return BSONComparator.getInstance().compare(((BSONWritable)a).getDoc(), ((BSONWritable)b).getDoc());
        }else{
            //return super.compare( a, b );
            return -1;
        }
    }

    public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 ){
        //return BSONComparator.getInstance().compare(b1, s1, l1, b2, s2, l2);
        return super.compare( b1, s1, l1, b2, s2, l2 );
    }

    public int compare( Object a, Object b ){
        return BSONComparator.getInstance().compare(((BSONWritable)a).getDoc(), ((BSONWritable)b).getDoc());
        //return super.compare( a, b );
    }

    private static final Log log = LogFactory.getLog( BSONWritableComparator.class );
}
