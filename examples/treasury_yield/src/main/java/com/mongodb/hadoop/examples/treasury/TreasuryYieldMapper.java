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
package com.mongodb.hadoop.examples.treasury;

// Mongo

import org.bson.*;
import com.mongodb.hadoop.util.*;

// Commons
import org.apache.commons.logging.*;

// Hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.io.BSONWritable;
// Java
import java.io.*;
import java.util.*;

/**
 * The treasury yield mapper.
 */
public class TreasuryYieldMapper
        extends Mapper<Object, BSONObject, IntWritable, DoubleWritable> {

    @Override
    public void map( final Object pKey,
                     final BSONObject pValue,
                     final Context pContext )
            throws IOException, InterruptedException{

        final int year = ((Date)pValue.get("_id")).getYear() + 1900;
        double bid10Year = ( (Number) pValue.get( "bc10Year" ) ).doubleValue();

        pContext.write( new IntWritable( year ), new DoubleWritable( bid10Year ) );
    }

    private static final Log LOG = LogFactory.getLog( TreasuryYieldMapper.class );
}

