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

package com.mongodb.hadoop.pig;

import com.mongodb.*;
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.output.*;
import com.mongodb.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.*;

import java.io.*;
import java.util.*;

public class MongoStorage extends StoreFunc implements StoreMetadata {
    private static final Log log = LogFactory.getLog( MongoStorage.class );
    // Pig specific settings
    static final String PIG_OUTPUT_SCHEMA = "mongo.pig.output.schema";
    static final String PIG_OUTPUT_SCHEMA_UDF_CONTEXT = "mongo.pig.output.schema.udf_context";

    public MongoStorage(){ }


    public void checkSchema( ResourceSchema schema ) throws IOException{
        final Properties properties =
                UDFContext.getUDFContext().getUDFProperties( this.getClass(), new String[] { _udfContextSignature } );
        properties.setProperty( PIG_OUTPUT_SCHEMA_UDF_CONTEXT, parseSchema( schema ) );
    }

    public String parseSchema( ResourceSchema schema ){
        final StringBuilder fields = new StringBuilder();
        for ( final String field : schema.fieldNames() ){
            fields.append( field );
            fields.append( "," );
        }
        return fields.substring( 0, fields.length() - 1 );
    }


    public void storeSchema( ResourceSchema schema, String location, Job job ){
        // not implemented
    }


    public void storeStatistics( ResourceStatistics stats, String location, Job job ){
        // not implemented
    }


    public void putNext( Tuple tuple ) throws IOException{
        final Configuration config = _recordWriter.getContext().getConfiguration();
        final List<String> schema = Arrays.asList( config.get( PIG_OUTPUT_SCHEMA ).split( "," ) );
        log.info( "Stored Schema: " + schema );
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        for ( int i = 0; i < tuple.size(); i++ ){
            log.info( "I: " + i + " tuple: " + tuple );
            builder.add( schema.get( i ), tuple.get( i ) );
        }
        _recordWriter.write( null, builder.get() );
    }


    public void prepareToWrite( RecordWriter writer ) throws IOException{
        _recordWriter = (MongoRecordWriter) writer;
        log.info( "Preparing to write to " + _recordWriter );
        if ( _recordWriter == null )
            throw new IOException( "Invalid Record Writer" );
    }


    public OutputFormat getOutputFormat() throws IOException{
        final MongoOutputFormat outputFmt = new MongoOutputFormat();
        log.info( "OutputFormat... " + outputFmt );
        return outputFmt;
    }


    public String relToAbsPathForStoreLocation( String location, org.apache.hadoop.fs.Path curDir ) throws IOException{
        // Don't convert anything - override to keep base from messing with URI
        log.info( "Converting path: " + location + "(curDir: " + curDir + ")" );
        return location;
    }


    public void setStoreLocation( String location, Job job ) throws IOException{
        final Configuration config = job.getConfiguration();
        log.info( "Store Location Config: " + config + " For URI: " + location );
        if ( !location.startsWith( "mongodb://" ) )
            throw new IllegalArgumentException(
                    "Invalid URI Format.  URIs must begin with a mongodb:// protocol string." );
        MongoConfigUtil.setOutputURI( config, location );
        final Properties properties =
                UDFContext.getUDFContext().getUDFProperties( this.getClass(), new String[] { _udfContextSignature } );
        config.set( PIG_OUTPUT_SCHEMA, properties.getProperty( PIG_OUTPUT_SCHEMA_UDF_CONTEXT ) );
    }


    public void setStoreFuncUDFContextSignature( String signature ){
        _udfContextSignature = signature;
    }

    String _udfContextSignature = null;
    MongoRecordWriter _recordWriter = null;

}
