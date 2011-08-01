package com.mongodb.hadoop.io;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;

/**
 * Copyright (c) 2010, 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

public class BSONComparator extends WritableComparator {
    public BSONComparator(){
        super( BSONWritable.class, true );
    }

    protected BSONComparator( Class<? extends WritableComparable> keyClass ){
        super( keyClass, true );
    }

    protected BSONComparator( Class<? extends WritableComparable> keyClass, boolean createInstances ){
        super( keyClass, createInstances );
    }

    public Class<? extends WritableComparable> getKeyClass(){
        return super.getKeyClass();
    }

    public WritableComparable newKey(){
        return super.newKey();
    }

    public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 ){
        return super.compare( b1, s1, l1, b2, s2, l2 );
    }

    public int compare( WritableComparable a, WritableComparable b ){
        if ( a instanceof BSONWritable && b instanceof BSONWritable ){
            return ( (BSONWritable) a )._doc.toString().compareTo( ( (BSONWritable) b )._doc.toString() );
        }
        else{
            return -1;
        }
    }

    public int compare( Object a, Object b ){
        return super.compare( a, b );
    }

    private static final Log log = LogFactory.getLog( BSONComparator.class );
}
