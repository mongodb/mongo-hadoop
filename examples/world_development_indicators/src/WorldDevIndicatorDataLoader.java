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
package com.mongodb.hadoop.examples;

// Mongo
import com.mongodb.Mongo;
import com.mongodb.DBAddress;
import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;

// Java
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * The world development indicator data loader.
 */
public class WorldDevIndicatorDataLoader {

    public static void main( final String [] pArgs ) throws Exception {

        final Mongo mongo = new Mongo( new DBAddress( "127.0.0.1:27017", "test" ) );

        mongo.getDB( "test" ).getCollection( "worldDevelopmentIndicators.in" ).remove( new BasicDBObject() );

        final DataInputStream in = new DataInputStream( new FileInputStream( DATA_FILE ) );
        final BufferedReader br = new BufferedReader(new InputStreamReader(in));

        final HashMap<Integer, String> fieldPositions = new HashMap<Integer, String>();

        try {
            String line;
            int count = 0;
            while ( ( line = br.readLine() ) != null ) {

                int position = 0;

                // If this is the first line, read the field positions.
                if ( count == 0 ) {

                    for ( final String field : line.split( "," ) ) {
                        fieldPositions.put( position++, field );
                    }

                    count++;
                    continue;
                }

                final BasicDBObject doc = new BasicDBObject();

                doc.put("_id", ObjectId.get().toString());

                for ( final String data : line.split( "," ) ) {

                    final String field = fieldPositions.get( position++ );

                    if ( field == null) continue;

                    if ( data == null || data.equals( "" ) ) continue;

                    // Check to see if this is a number.
                    try {
                        doc.put( field, Double.parseDouble( data ) );
                    } catch ( final NumberFormatException nfe ) {
                        // This is a string.
                        doc.put( field, data );
                    }
                }

                mongo.getDB( "test" ).getCollection( "worldDevelopmentIndicators.in" ).insert( doc );

            }
        } finally { if (in != null) in.close(); }
    }

    private static final String DATA_FILE
    = "examples/world_development_indicators/resources/WDI_GDF_Data.csv";
}

