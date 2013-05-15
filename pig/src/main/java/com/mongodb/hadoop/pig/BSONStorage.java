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

public class BSONStorage extends StoreFunc implements StoreMetadata {

    private static final Log log = LogFactory.getLog( MongoStorage.class );
    static final String SCHEMA_SIGNATURE = "bson.pig.output.schema";
    protected ResourceSchema schema = null;
    private RecordWriter out;

    private String udfcSignature = null;
    private String idField = null;
    private boolean useUpsert = false; 

    private final BSONFileOutputFormat outputFormat = new BSONFileOutputFormat();

    public BSONStorage(){ }

    public BSONStorage(String idField){ 
        this.idField = idField;
    }

    public static Object getTypeForBSON(Object o, ResourceSchema.ResourceFieldSchema field) throws IOException{
        byte dataType = field != null ? field.getType() : DataType.UNKNOWN;
        ResourceSchema s = null;
        if( field == null ){
            if(o instanceof Map){
                dataType = DataType.MAP;
            }else if(o instanceof List){ 
                dataType = DataType.BAG;
            } else {
                dataType = DataType.UNKNOWN;
            }
        }else{
            s = field.getSchema();
            if(dataType == DataType.UNKNOWN ){
                if(o instanceof Map) dataType = DataType.MAP;
                if(o instanceof List) dataType = DataType.BAG;
            }
        }

        if(dataType == DataType.BYTEARRAY && o instanceof Map){
            dataType = DataType.MAP;
        }

        switch (dataType) {
            case DataType.NULL:
                return null;
            case DataType.INTEGER:
            case DataType.LONG:
            case DataType.FLOAT:
            case DataType.DOUBLE:
                return o;
            case DataType.BYTEARRAY:
                return o.toString();
            case DataType.CHARARRAY:
                return (String)o;

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
                    m.put(fs[j].getName(), getTypeForBSON(((Tuple) o).get(j), fs[j])); 
                }
                return m;

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
                for (Tuple t : (DataBag)o) {
                    LinkedHashMap ma = new java.util.LinkedHashMap();
                    for (int j = 0; j < fs.length; j++) {
                        ma.put(fs[j].getName(), ((Tuple) t).get(j));
                    }
                    a.add(ma);
                }

                return a;
            case DataType.MAP:
                Map map = (Map) o;
                Map<String,Object> out = new HashMap<String,Object>(map.size());
                for(Object key : map.keySet()) {
                    out.put(key.toString(), getTypeForBSON(map.get(key), null));
                }
                return out;
            default:
                return o;
        }
    }

    protected void writeField(BasicDBObjectBuilder builder,
                            ResourceSchema.ResourceFieldSchema field,
                            Object d) throws IOException {
        Object convertedType = getTypeForBSON(d, field);
        String fieldName = field != null ? field.getName() : "value";

        if(convertedType instanceof Map){
            for( Map.Entry<String, Object> mapentry : ((Map<String,Object>)convertedType).entrySet() ){
                String addKey = mapentry.getKey().equals(this.idField) ? "_id" : mapentry.getKey();
                builder.add(addKey, mapentry.getValue());
            }
        }else{
            String addKey =  field!=null && fieldName.equals(this.idField) ? "_id" : fieldName;
            builder.add(fieldName, convertedType);
        }
        
    }
    
    public void checkSchema( ResourceSchema schema ) throws IOException{
        this.schema = schema;
        UDFContext udfc = UDFContext.getUDFContext();

        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty(SCHEMA_SIGNATURE, schema.toString());
    }

    public void storeSchema( ResourceSchema schema, String location, Job job ){
        // not implemented
    }


    public void storeStatistics( ResourceStatistics stats, String location, Job job ){
        // not implemented
    }

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
            log.warn("Could not find schema in UDF context!");
            log.warn("Will attempt to write records without schema.");
        }

        try {
            // Parse the schema from the string stored in the properties object.
            this.schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        } catch (Exception e) {
            this.schema = null;
            log.warn(e.getMessage());
        }

    }

    public OutputFormat getOutputFormat() throws IOException{
        return this.outputFormat;
    }

    public String relToAbsPathForStoreLocation( String location, org.apache.hadoop.fs.Path curDir ) throws IOException{
        return LoadFunc.getAbsolutePath(location, curDir);
    }

    public void setStoreLocation( String location, Job job ) throws IOException{
        final Configuration config = job.getConfiguration();
        config.set("mapred.output.file", location);
    }


    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        udfcSignature = signature;
    }

}
