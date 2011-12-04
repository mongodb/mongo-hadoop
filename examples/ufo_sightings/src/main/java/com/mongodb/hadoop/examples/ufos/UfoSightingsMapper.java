/*
 * Copyright 2011 10gen Inc.
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
package com.mongodb.hadoop.examples.ufos;

// Mongo

import org.bson.*;
import org.bson.types.*;
import com.mongodb.hadoop.util.*;

// Commons
import org.apache.commons.logging.*;

// Hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Java
import java.io.*;
import java.util.*;

/**
 * The UFO Sightings yield mapper.
 */

public class UfoSightingsMapper 
        extends Mapper<ObjectId, BSONObject, Text, IntWritable> {

    @Override
    public void map( final ObjectId oid,
                     final BSONObject doc,
                     final Context pContext )
            throws IOException, InterruptedException{

        String shape = ( (String) doc.get( "shape" ) );
        String location = ( (String) doc.get( "location" ) );

        pContext.write( new Text( location ), new IntWritable( 1 ) );
    }

    private static final Log LOG = LogFactory.getLog( UfoSightingsMapper.class );
}
