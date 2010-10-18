package com.mongodb.hadoop.pig;

import java.io.*;

import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.output.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.impl.util.UDFContext;

import java.util.List;
import java.util.Properties;
import java.util.Arrays;



public class MongoStorage extends StoreFunc implements StoreMetadata {

  public MongoStorage() {
  }

  private String udfContextSignature = null;
  private MongoRecordWriter recordWriter = null;
  public final static String CONF_OUTPUT_URI = "MONGO_OUTPUT";
  public final static String CONF_OUTPUT_SCHEMA = "MONGO_SCHEMA";
  public final static String UDFCONTEXT_OUTPUT_SCHEMA = "UDF.MONGO_SCHEMA";
  
  @Override
  public void checkSchema(ResourceSchema schema) throws IOException {
    Properties properties = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] { udfContextSignature }); 
    properties.setProperty(UDFCONTEXT_OUTPUT_SCHEMA, parseSchema(schema));
  }

  public String parseSchema(ResourceSchema schema) {
    StringBuilder fields = new StringBuilder();
    for (String field : schema.fieldNames()) {
      fields.append(field); 
      fields.append(",");
    }
    return fields.substring(0, fields.length() - 1);
  }

  @Override 
  public void storeSchema(ResourceSchema schema, String location, Job job) {
    // not implemented
  }

  @Override 
  public void storeStatistics(ResourceStatistics stats, String location, Job job) {
    // not implemented
  }
  @Override 
  public void putNext(Tuple tuple) throws IOException {
    Configuration config = recordWriter.getContext().getConfiguration();
    List<String> schema = Arrays.asList(config.get(CONF_OUTPUT_SCHEMA).split(","));
    System.out.println("Stored Schema: " + schema);
    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
    for (int i = 0; i < tuple.size(); i++) {
      builder.add(schema.get(i), tuple.get(i));
    }
    recordWriter.write(null, builder.get());
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    recordWriter = (MongoRecordWriter) writer;
    if (recordWriter == null) {
        throw new IOException("Invalid Record Writer");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new MongoOutputFormat();
  }
  
  @Override 
  public String relToAbsPathForStoreLocation(String location, org.apache.hadoop.fs.Path curDir) throws IOException {
    // Don't convert anything - override to keep base from messing with URI
    return location;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    Configuration config = job.getConfiguration();
    if (!location.startsWith("mongodb://")) 
      throw new IllegalArgumentException("Invalid URI Format.  URIs must begin with a mongodb:// protocol string.");
    config.set(CONF_OUTPUT_URI, location);
    Properties properties = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] { udfContextSignature });
    config.set(CONF_OUTPUT_SCHEMA, UDFCONTEXT_OUTPUT_SCHEMA);
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    udfContextSignature = signature;
  }
  
}
