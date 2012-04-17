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
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;


import java.io.*;
import java.text.ParseException;
import java.util.*;

public class MongoStorage extends StoreFunc implements StoreMetadata {
    private static final Log log = LogFactory.getLog( MongoStorage.class );
    // Pig specific settings
    static final String PIG_OUTPUT_SCHEMA = "mongo.pig.output.schema";
    static final String PIG_OUTPUT_SCHEMA_UDF_CONTEXT = "mongo.pig.output.schema.udf_context";
    protected ResourceSchema schema = null;
    private final MongoStorageOptions options;

    public MongoStorage(){ 
        this.options = null;
    }
    
    public MongoStorage(String... args) throws ParseException {
        this.options = MongoStorageOptions.parseArguments(args);
    }


    public void checkSchema( ResourceSchema schema ) throws IOException{
        final Properties properties =
                UDFContext.getUDFContext().getUDFProperties( this.getClass(), new String[] { _udfContextSignature } );
        properties.setProperty( PIG_OUTPUT_SCHEMA_UDF_CONTEXT, schema.toString());
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
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

        ResourceFieldSchema[] fields = this.schema.getFields();
        for (int i = 0; i < fields.length; i++) {
            writeField(builder, fields[i], tuple.get(i));
        }
        _recordWriter.write( null, builder.get() );
    }

    private void writeField(BasicDBObjectBuilder builder,
                            ResourceSchema.ResourceFieldSchema field,
                            Object d) throws IOException {

        // If the field is missing or the value is null, write a null
        if (d == null) {
            builder.add( field.getName(), d );
            return;
        }

        ResourceSchema s = field.getSchema();

        // Based on the field's type, write it out
        switch (field.getType()) {
            case DataType.INTEGER:
                builder.add( field.getName(), (Integer)d );
                return;

            case DataType.LONG:
                builder.add( field.getName(), (Long)d );
                return;

            case DataType.FLOAT:
                builder.add( field.getName(), (Float)d );
                return;

            case DataType.DOUBLE:
                builder.add( field.getName(), (Double)d );
                return;

            case DataType.BYTEARRAY:
                builder.add( field.getName(), d.toString() );
                return;

            case DataType.CHARARRAY:
                builder.add( field.getName(), (String)d );
                return;

            // Given a TUPLE, create a Map so BSONEncoder will eat it
            case DataType.TUPLE:
                if (s == null) {
                    throw new IOException("Schemas must be fully specified to use "
                            + "this storage function.  No schema found for field " +
                            field.getName());
                }
                ResourceSchema.ResourceFieldSchema[] fs = s.getFields();
                LinkedHashMap m = new java.util.LinkedHashMap();
                for (int j = 0; j < fs.length; j++) {
                    m.put(fs[j].getName(), ((Tuple) d).get(j));
                }
                builder.add( field.getName(), (Map)m );
                return;

            // Given a BAG, create an Array so BSONEnconder will eat it.
            case DataType.BAG:
                if (s == null) {
                    throw new IOException("Schemas must be fully specified to use "
                            + "this storage function.  No schema found for field " +
                            field.getName());
                }
                fs = s.getFields();
                if (fs.length != 1 || fs[0].getType() != DataType.TUPLE) {
                    throw new IOException("Found a bag without a tuple "
                            + "inside!");
                }
                // Drill down the next level to the tuple's schema.
                s = fs[0].getSchema();
                if (s == null) {
                    throw new IOException("Schemas must be fully specified to use "
                            + "this storage function.  No schema found for field " +
                            field.getName());
                }
                fs = s.getFields();

                ArrayList a = new ArrayList<Map>();
                for (Tuple t : (DataBag)d) {
                    LinkedHashMap ma = new java.util.LinkedHashMap();
                    for (int j = 0; j < fs.length; j++) {
                        ma.put(fs[j].getName(), ((Tuple) t).get(j));
                    }
                    a.add(ma);
                }

                builder.add( field.getName(), a);
                return;
        }
    }

    public void prepareToWrite( RecordWriter writer ) throws IOException{

        _recordWriter = (MongoRecordWriter) writer;
        log.info( "Preparing to write to " + _recordWriter );
        if ( _recordWriter == null )
            throw new IOException( "Invalid Record Writer" );
        // Parse the schema from the string stored in the properties object.

        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
                udfc.getUDFProperties(this.getClass(), new String[]{_udfContextSignature});

        String strSchema = p.getProperty(PIG_OUTPUT_SCHEMA_UDF_CONTEXT);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        try {
        // Parse the schema from the string stored in the properties object.
            schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        // If we are insuring any indexes do so now:
        for (MongoStorageOptions.Index in : options.getIndexes()) {
            _recordWriter.ensureIndex(in.index, in.options);
        }
    }

    public OutputFormat getOutputFormat() throws IOException{
        final MongoOutputFormat outputFmt = new MongoOutputFormat(options.getUpdate().keys, options.getUpdate().multi);
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
