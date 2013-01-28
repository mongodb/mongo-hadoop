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
    
    /**
     * Takes a list of arguments of two types: 
     * <ul>
     * <li>A single set of keys to base updating on in the format:<br />
     * 'update [time, user]' or 'multi [timer, user] for multi updates</li>
     * 
     * <li>Multiple indexes to ensure in the format:<br />
     * '{time: 1, user: 1},{unique: true}'<br />
     * (The syntax is exactly like db.col.ensureIndex())</li>
     * </ul>
     * Example:<br />
     * STORE Result INTO '$db' USING com.mongodb.hadoop.pig.MongoStorage('update [time, servername, hostname]', '{time : 1, servername : 1, hostname : 1}, {unique:true, dropDups: true}').
     * @param args
     * @throws ParseException
     */
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

        BasicDBObject record = new BasicDBObject();
        ResourceFieldSchema[] fields = this.schema.getFields();

        for (int i = 0; i < fields.length; i++) {
            writeField(record, fields[i], tuple.get(i));
        }
        
        _recordWriter.write( null, record );
    }

    protected void writeField(BasicDBObject obj,
                              ResourceSchema.ResourceFieldSchema field,
                              Object d) throws IOException {

        // If the field is missing or the value is null, write a null
        if (d == null) {
            obj.append( field.getName(), d );
            return;
        }

        ResourceSchema s = field.getSchema();
        ResourceFieldSchema[] fs = (s == null? null : s.getFields());
        BasicDBObject nestedObj;

        // Based on the field's type, write it out
        switch (field.getType()) {
            case DataType.INTEGER:
                if (d instanceof Integer)
                    obj.append( field.getName(), (Integer) d );
                else if (d instanceof DataByteArray)
                    obj.append( field.getName(), Integer.parseInt(d.toString()));
                else {
                    throw new IOException(
                        "Schema lists field " + field.getName() + 
                        "as an int, but value was not an int or bytearray.");
                }
                return;

            case DataType.LONG:
                if (d instanceof Long)
                    obj.append( field.getName(), (Long) d );
                else if (d instanceof DataByteArray)
                    obj.append( field.getName(), Long.parseLong(d.toString()));
                else {
                    throw new IOException(
                        "Schema lists field " + field.getName() + 
                        "as a long, but value was not a long or bytearray.");
                }
                return;

            case DataType.FLOAT:
                if (d instanceof Float)
                    obj.append( field.getName(), (Float) d );
                else if (d instanceof DataByteArray)
                    obj.append( field.getName(), Float.parseFloat(d.toString()));
                else {
                    throw new IOException(
                        "Schema lists field " + field.getName() + 
                        "as a float, but value was not a float or bytearray.");
                }
                return;

            case DataType.DOUBLE:
                if (d instanceof Double)
                    obj.append( field.getName(), (Double) d );
                else if (d instanceof DataByteArray)
                    obj.append( field.getName(), Double.parseDouble(d.toString()));
                else {
                    throw new IOException(
                        "Schema lists field " + field.getName() + 
                        "as a double, but value was not a double or bytearray.");
                }
                return;

            case DataType.BYTEARRAY:
                obj.append( field.getName(), d.toString() );
                return;

            case DataType.CHARARRAY:
                obj.append( field.getName(), (String) d );
                return;

            case DataType.MAP:
                Map map = (Map) d;
                nestedObj = new BasicDBObject();

                if (fs != null && fs.length == 1) {
                    ResourceFieldSchema nestedField = fs[0];
                    for(Object key : map.keySet()) {
                        nestedField.setName(key.toString());
                        writeField(nestedObj, nestedField, map.get(key));
                    }
                } else {
                    // infer schema
                    for (Object key : map.keySet()) {
                        Object nestedData = map.get(key);
                        ResourceFieldSchema nestedField = new ResourceFieldSchema();
                        
                        nestedField.setName(key.toString());
                        nestedField.setType(inferType(nestedData, key.toString(), field.getName()));
                        
                        writeField(nestedObj, nestedField, nestedData);
                    }
                }

                obj.append ( field.getName(), nestedObj );
                return;

            case DataType.TUPLE:
                Tuple tData = (Tuple) d;
                nestedObj = new BasicDBObject();

                if (fs != null) {
                    for (int j = 0; j < fs.length; j++) {
                        writeField(nestedObj, fs[j], tData.get(j));
                    }
                } else {
                    for (int j = 0; j < tData.size(); j++) {
                        Object nestedData = tData.get(j);
                        ResourceFieldSchema nestedField = new ResourceFieldSchema();
                        
                        String key = "" + j;
                        nestedField.setName(key);
                        nestedField.setType(inferType(nestedData, key, field.getName()));
                        
                        writeField(nestedObj, nestedField, nestedData);
                    }
                }
                
                obj.append( field.getName(), nestedObj );
                return;

            case DataType.BAG:
                boolean hasSchema = false;
                try {
                    // Drill down the next level to the tuple's schema.
                    s = fs[0].getSchema();
                    fs = s.getFields();
                    hasSchema = true;
                } catch (Exception e) {
                    // we don't throw an exception since this is just to test if a schema is specified
                    // unspecified schemas are handled below (this is so that bag values in map[] schemas work correctly)
                }

                DataBag bag = (DataBag) d;
                BasicDBList arr = new BasicDBList();

                int i = 0;
                for (Tuple t : bag) {
                    nestedObj = new BasicDBObject();
                    
                    if (hasSchema) {
                        for (int j = 0; j < fs.length; j++) {
                            writeField(nestedObj, fs[j], t.get(j));
                        }
                    } else {
                        for (int j = 0; j < t.size(); j++) {
                            Object nestedData = t.get(j);
                            ResourceFieldSchema nestedField = new ResourceFieldSchema();
                            
                            String key = "" + j;
                            nestedField.setName(key);
                            nestedField.setType(inferType(nestedData, key, field.getName()));
                            
                            writeField(nestedObj, nestedField, t.get(j));
                        }
                    }
                    
                    arr.put(i, nestedObj);
                    i++;
                }

                obj.append( field.getName(), arr );
                return;
        }
    }
    
    private byte inferType(Object o, String key, String fieldName)
                 throws IOException {
        
        if (o instanceof Integer) {
           return DataType.INTEGER;
        } else if (o instanceof Long) {
            return DataType.LONG;
        } else if (o instanceof Float) {
            return DataType.FLOAT;
        } else if (o instanceof Double) {
            return DataType.DOUBLE;
        } else if (o instanceof String) {
            return DataType.CHARARRAY;
        } else if (o instanceof DataByteArray) {
            return DataType.BYTEARRAY;
        } else if (o instanceof Map) {
            return DataType.MAP;
        } else if (o instanceof Tuple) {
            return DataType.TUPLE;
        } else if (o instanceof DataBag) {
           return DataType.BAG;
        } else {
            throw new IOException(
                "Failed to infer type of value for key \"" + key + "\" " +
                "in schema-less map field " + fieldName + "."
            );
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
        
        if(options != null) {
            // If we are insuring any indexes do so now:
            for (MongoStorageOptions.Index in : options.getIndexes()) {
                _recordWriter.ensureIndex(in.index, in.options);
            }
        }
    }

    public OutputFormat getOutputFormat() throws IOException{
        final MongoOutputFormat outputFmt = options == null ? new MongoOutputFormat() : new MongoOutputFormat(options.getUpdate().keys, options.getUpdate().multi);
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
    
    public void cleanupOnFailure(String location, Job job) throws IOException {
        log.error("Store operation failed (see logged exception). Your Mongo collection will retain any records inserted or updated by MongoStorage before it failed.");
    }

    String _udfContextSignature = null;
    MongoRecordWriter _recordWriter = null;
}
