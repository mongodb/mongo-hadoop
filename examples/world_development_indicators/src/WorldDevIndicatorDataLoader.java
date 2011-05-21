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
import java.util.List;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The world development indicator data loader. Loads the CSV data into Mongo.
 */
public class WorldDevIndicatorDataLoader {

    public static void main( final String [] pArgs ) throws Exception {

        final Mongo mongo = new Mongo( new DBAddress( "127.0.0.1:27017", "test" ) );

        mongo.getDB( "test" ).getCollection( "worldDevelopmentIndicators.in" ).remove( new BasicDBObject() );

        final DataInputStream in = new DataInputStream( new FileInputStream( DATA_FILE ) );
        final BufferedReader br = new BufferedReader(new InputStreamReader(in));

        final HashMap<Integer, String> fieldPositions = new HashMap<Integer, String>();

        _csvPattern = Pattern.compile(CSV_REGEXP);

        try {

            final LinkedList<String> vals = new LinkedList<String>();

            String line;
            int count = 0;
            while ( ( line = br.readLine() ) != null ) {

                int position = 0;

                // If this is the first line, read the field positions.
                if ( count == 0 ) {
                    for ( final String field : line.split( "," ) )
                        fieldPositions.put( position++, field );

                    count++;
                    continue;
                }

                final BasicDBObject doc = new BasicDBObject();

                doc.put("_id", ObjectId.get().toString());

                // Loop through the data and insert.
                parseCsvLine( line, vals );

                if (vals.isEmpty()) continue;

                for ( final String data : vals ) {

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

    /**
     * Parse the CSV line.
     */
    private static void parseCsvLine( final String pLine, final LinkedList<String> pVals ) {
        pVals.clear();
        final Matcher matcher = _csvPattern.matcher(pLine);

        while ( matcher.find() ) {
            String match = matcher.group();

            if (match == null) break;

            if ( match.endsWith( "," ) )
                match = match.substring( 0, match.length() - 1);

            if ( match.startsWith( "\"" ) )
                match = match.substring( 1, match.length() - 1 );

            if ( match.length() == 0 ) match = null;
            pVals.addLast( match );
        }
    }

    private static Pattern _csvPattern;

    private static final String CSV_REGEXP = "\"([^\"]+?)\",?|([^,]+),?|,";

    private static final String DATA_FILE
    = "examples/world_development_indicators/resources/WDI_GDF_Data.csv";
}

