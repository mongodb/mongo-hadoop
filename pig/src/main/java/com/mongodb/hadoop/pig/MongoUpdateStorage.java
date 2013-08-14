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
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.output.*;
import com.mongodb.hadoop.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.*;
import org.apache.pig.ResourceSchema;

import java.io.*;
import java.util.*;

/*
 * MongoUpdateStorage : used to update documents in a collection
 */
public class MongoUpdateStorage extends StoreFunc implements StoreMetadata {
    
    private static final Log log = LogFactory.getLog(MongoUpdateStorage.class);
    
    // Pig specific settings
    static final String SCHEMA_SIGNATURE = "mongoupdate.pig.output.schema";
    protected ResourceSchema schema = null;
    private String udfcSignature = null;
    
    // private final MongoStorageOptions options;
    private final MongoOutputFormat outputFormat = new MongoOutputFormat();
    
    // MongoRecordWriter to use for updating MongoDB documents
    MongoRecordWriter<?, MongoUpdateWritable> _recordWriter = null;
    
    // JSONPigReplace setup
    JSONPigReplace repl;
    private String schemaStr;
    private String unnamedStr;
    
    /*
     * First constructor
     * 
     * @param query : JSON string representing 'query' parameter in MongoDB update
     * @param update : JSON string representing 'update' parameter in MongoDB update
     */
    public MongoUpdateStorage(String query, String update) {
        repl = new JSONPigReplace(new String[] {query, update});
    }
    
    /*
     * Second constructor
     * 
     * @param query : JSON string representing 'query' parameter in MongoDB update
     * @param update : JSON string representing 'update' parameter in MongoDB update
     * @param String s : string representing schema of pig output
     */
    public MongoUpdateStorage(String query, String update, String s) {
        this(query, update);
        schemaStr = s;
    }
    
    /*
     * Third constructor
     * 
     * @param query : JSON string representing 'query' parameter in MongoDB update
     * @param update : JSON string representing 'update' parameter in MongoDB update
     * @param String s : string representing schema of pig output
     * @param toIgnore : string representing "unnamed" objects
     */
    public MongoUpdateStorage(String query, String update, String s, String toIgnore) {
        this(query, update, s); 
        unnamedStr = (toIgnore.length() > 0 ? toIgnore : null);
    }
    
    /*
     * Third constructor
     * 
     * @param query : JSON string representing 'query' parameter in MongoDB update
     * @param update : JSON string representing 'update' parameter in MongoDB update
     * @param String s : string representing schema of pig output
     * @param toIgnore : string representing "unnamed" objects
     * @param updateOptions : JSON string representing 'extra' MongoDB update options
     */
    public MongoUpdateStorage(String query, String update, String s, String toIgnore, String updateOptions) {
        repl = new JSONPigReplace(new String[] {query, update, updateOptions});
        schemaStr = s;
        unnamedStr = (toIgnore.length() > 0 ? toIgnore : null);
    }
    
    @Override
    public void checkSchema(ResourceSchema s) throws IOException{
        schema = s;
        UDFContext udfc = UDFContext.getUDFContext();
        
        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty(SCHEMA_SIGNATURE, schema.toString());
    }
    
    @Override
    public void storeSchema(ResourceSchema schema, String location, Job job){
        // not implemented
    }
    
    @Override
    public void storeStatistics(ResourceStatistics stats, String location, Job job){
        // not implemented
    }
    
    @Override
    public void putNext(Tuple tuple) throws IOException{
        try{
            // perform substitution on variables "marked" for replacements
            BasicBSONObject[] toUpdate = repl.substitute(tuple,  schema, unnamedStr);
            // 'query' JSON
            BasicBSONObject q = toUpdate[0];
            // 'update' JSON
            BasicBSONObject u = toUpdate[1];
            
            // multi and upsert 'options' JSON
            boolean isUpsert = true;
            boolean isMulti = false;
            BasicBSONObject mu = (toUpdate.length > 2 ? toUpdate[2] : null);
            if (mu != null) {
                isUpsert = (mu.containsField("upsert") ? mu.getBoolean("upsert") : isUpsert);
                isMulti = (mu.containsField("multi") ? mu.getBoolean("multi") : isMulti);
            }
            
            _recordWriter.write(null, new MongoUpdateWritable(q,u, 
                                                              isUpsert,
                                                              isMulti));
        }catch(Exception e){
            e.printStackTrace();
            throw new IOException("Couldn't convert tuple to bson: " , e);
        }
    }
    
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException{     
        _recordWriter = (MongoRecordWriter<?, MongoUpdateWritable>) writer;
        log.info( "Preparing to write to " + _recordWriter );
        if ( _recordWriter == null )
            throw new IOException( "Invalid Record Writer" );
        
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        
        /*
         * In determining the schema to use, the user-defined schema should take
         * precedence over the "inferred" schema
         */
        if (schemaStr != null ) {
            try {
                schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));
        }
            catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            String s = p.getProperty(SCHEMA_SIGNATURE);
            if (s == null) {
                throw new IOException("Could not find schema in UDF context. You'd have to explicitly specify a Schema.");
            }
            schema = new ResourceSchema(Utils.getSchemaFromString(s));
        }
    }
    
    @Override
    public OutputFormat getOutputFormat() throws IOException{
        return outputFormat;
    }
    
    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
        return location;
    }
    
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        final Configuration config = job.getConfiguration();
        log.info("Store Location Config: " + config + "; For URI: " + location);
        
        if ( !location.startsWith("mongodb://") )
            throw new IllegalArgumentException("Invalid URI Format.  URIs must begin with a mongodb:// protocol string.");
        
        // set output URI
        MongoConfigUtil.setOutputURI(config, location);
    }
    
    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        udfcSignature = signature;
    }
}
