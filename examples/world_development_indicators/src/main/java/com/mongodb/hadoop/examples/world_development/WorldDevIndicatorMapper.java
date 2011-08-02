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
package com.mongodb.hadoop.examples.world_development;

// Mongo

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

// Commons
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

// Java
import java.io.IOException;

/**
 * The world development indicator mapper. This example looks at:
 *
 * <pre>
 * SeriesCode : SL.GDP.PCAP.EM.KD
 * Series Name : GDP per person employed (constant 1990 PPP $)
 * </pre>
 *
 * From 1980 - 2008;
 *
 * From the data, it calculates the average growth rate per year.
 */
public class WorldDevIndicatorMapper
        extends Mapper<String, BSONObject, Text, DoubleWritable> {
    public void map( final String pKey,
                     final BSONObject pValue,
                     final Context pContext )
            throws IOException, InterruptedException{
        final BasicBSONObject doc = (BasicBSONObject) pValue;

        if ( !doc.getString( "SeriesCode" ).equals( "SL.GDP.PCAP.EM.KD" ) ) return;

        int yearCount = 0;

        double sum = 0;

        double previous = -1;
        double current = 0;

        for ( int year = START_YEAR; year <= END_YEAR; year++ ){

            final String yearField = Integer.toString( year );

            if ( !doc.containsField( yearField ) ) continue;

            current = doc.getDouble( yearField );

            yearCount++;

            if ( previous == -1 ){
                previous = current;
                continue;
            }

            sum += ( ( current - previous ) / previous * (double) 100 );

            previous = current;
        }

        if ( yearCount == 0 ) return;

        final DoubleWritable value = new DoubleWritable( ( sum / (double) yearCount ) );

        pContext.write( new Text( doc.getString( "Country Name" ) ), value );
    }

    private static final int START_YEAR = 1980;
    private static final int END_YEAR = 2008;

    private static final Log LOG = LogFactory.getLog( WorldDevIndicatorMapper.class );
}

