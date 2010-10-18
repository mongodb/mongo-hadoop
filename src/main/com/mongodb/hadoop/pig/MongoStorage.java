package com.mongodb.hadoop.pig;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.output.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.impl.util.UDFContext;

import java.util.List;
import java.util.Properties;
import java.util.Arrays;

public class MongoStorage extends StoreFunc implements StoreMetadata {
  private static final Log log =
    LogFactory.getLog(MongoStorage.class);

  private String udfContextSignature = null;
  private MongoRecordWriter recordWriter = null;

  public final static String CONF_OUTPUT_URI = "MONGO_OUTPUT";
  public final static String CONF_OUTPUT_SCHEMA = "MONGO_SCHEMA";
  public final static String UDFCONTEXT_OUTPUT_SCHEMA = "UDF.MONGO_SCHEMA";
  
  public MongoStorage() { }

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
    log.info("Stored Schema: " + schema);
    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
    for (int i = 0; i < tuple.size(); i++) {
      log.info("I: " + i + " tuple: " + tuple);
      builder.add(schema.get(i), tuple.get(i));
    }
    recordWriter.write(null, builder.get());
  }
  
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    recordWriter = (MongoRecordWriter) writer;
    log.info("Preparing to write to " + recordWriter);
    if (recordWriter == null) {
        throw new IOException("Invalid Record Writer");
    }
  }

  @Override
  public OutputFormat getOutputFormat() throws IOException {
    MongoOutputFormat outputFmt = new MongoOutputFormat();
    log.info("OutputFormat... "  + outputFmt);
    return outputFmt;
  }
  
  @Override 
  public String relToAbsPathForStoreLocation(String location, org.apache.hadoop.fs.Path curDir) throws IOException {
    // Don't convert anything - override to keep base from messing with URI
    log.info("Converting path: " + location + "(curDir: " + curDir + ")");
    return location;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    Configuration config = job.getConfiguration();
    log.info("Store Location Config: " + config + " For URI: " + location);
    if (!location.startsWith("mongodb://")) 
      throw new IllegalArgumentException("Invalid URI Format.  URIs must begin with a mongodb:// protocol string.");
    log.info("Storing " + location + " in " + CONF_OUTPUT_URI);
    config.set(CONF_OUTPUT_URI, location);
    Properties properties = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] { udfContextSignature });
    config.set(CONF_OUTPUT_SCHEMA, properties.getProperty(UDFCONTEXT_OUTPUT_SCHEMA));
    log.info("Config: " + config);
    log.info("Config Item " + CONF_OUTPUT_URI + " is " + config.get(CONF_OUTPUT_URI));
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    udfContextSignature = signature;
  }
  
}
