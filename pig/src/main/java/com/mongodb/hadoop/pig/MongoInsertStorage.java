
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

import org.bson.*;
import org.bson.types.*;
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

public class MongoInsertStorage extends StoreFunc implements StoreMetadata {

    private static final Log log = LogFactory.getLog( MongoStorage.class );
    // Pig specific settings
    static final String SCHEMA_SIGNATURE = "mongoinsert.pig.output.schema";
    protected ResourceSchema schema = null;
    private RecordWriter out;

    private String udfcSignature = null;
    private String idField = null;
    private boolean useUpsert = false; 

    private final MongoOutputFormat outputFormat = new MongoOutputFormat();

    public MongoInsertStorage(){ 
    }

    public MongoInsertStorage(String idField, String useUpsert){ 
        this.idField = idField;
        this.useUpsert = useUpsert == null ? false : useUpsert.toLowerCase().trim().equals("true");
    }

    protected void writeField(BasicDBObjectBuilder builder,
                            ResourceSchema.ResourceFieldSchema field,
                            Object d) throws IOException {
        Object convertedType = BSONStorage.getTypeForBSON(d, field);
        if(field.getName() != null && field.getName().equals(this.idField)){
            builder.add("_id", convertedType);
            return;
        } else {
            builder.add(field.getName(), convertedType);
        }
        
    }
    
    @Override
    public void checkSchema( ResourceSchema schema ) throws IOException{
        this.schema = schema;
        UDFContext udfc = UDFContext.getUDFContext();

        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty(SCHEMA_SIGNATURE, schema.toString());
    }

    @Override
    public void storeSchema( ResourceSchema schema, String location, Job job ){
        // not implemented
    }

    @Override
    public void storeStatistics( ResourceStatistics stats, String location, Job job ){
        // not implemented
    }

    @Override
    public void putNext( Tuple tuple ) throws IOException{
        try{
            final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
            ResourceFieldSchema[] fields = null;
            if(this.schema != null){
                fields = this.schema.getFields();
            }
            if(fields != null){
                for (int i = 0; i < fields.length; i++) {
                    writeField(builder, fields[i], tuple.get(i));
                }
            }else{
                for (int i = 0; i < tuple.size(); i++) {
                    writeField(builder, null, tuple.get(i));
                }
            }

            BSONObject bsonformat = builder.get();
            this.out.write(null, bsonformat);
        }catch(Exception e){
            e.printStackTrace();
            throw new IOException("Couldn't convert tuple to bson: " , e);
        }
    }

    public void prepareToWrite( RecordWriter writer ) throws IOException{
        this.out = writer;
        if ( this.out == null )
            throw new IOException( "Invalid Record Writer" );

        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        String strSchema = p.getProperty(SCHEMA_SIGNATURE);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        try {
            // Parse the schema from the string stored in the properties object.
            this.schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        } catch (Exception e) {
            this.schema = null;
            log.warn(e.getMessage());
        }

        log.info("GOT A SCHEMA "+ this.schema + " " + strSchema);
    }

    public OutputFormat getOutputFormat() throws IOException{
        return this.outputFormat;
        //final MongoOutputFormat outputFmt = options == null ? new MongoOutputFormat() : new MongoOutputFormat(options.getUpdate().keys, options.getUpdate().multi);
        //log.info( "OutputFormat... " + outputFmt );
        //return outputFmt;
    }

    public String relToAbsPathForStoreLocation( String location, org.apache.hadoop.fs.Path curDir ) throws IOException{
        // Don't convert anything - override to keep base from messing with URI
        return location;
    }

    public void setStoreLocation( String location, Job job ) throws IOException{
        final Configuration config = job.getConfiguration();
        log.info( "Store Location Config: " + config + " For URI: " + location );
        if ( !location.startsWith( "mongodb://" ) )
            throw new IllegalArgumentException(
                    "Invalid URI Format.  URIs must begin with a mongodb:// protocol string." );
        MongoConfigUtil.setOutputURI( config, location );
    }


    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        udfcSignature = signature;
    }

}
