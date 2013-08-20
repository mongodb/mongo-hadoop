package com.mongodb.hadoop.hive;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.mongodb.hadoop.mapred.BSONFileInputFormat;
import com.mongodb.hadoop.hive.output.HiveBSONStorageHandlerFileOutputFormat;

import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;


public class BSONStorageHandler extends DefaultStorageHandler {

    public BSONStorageHandler() {}

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return BSONFileInputFormat.class;
    }
    
    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HiveBSONStorageHandlerFileOutputFormat.class;
    }
 
    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return BSONSerDe.class;
    }
    
    @Override
    public void configureInputJobProperties(TableDesc tableDesc,
            Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        copyJobProperties(properties, jobProperties);
    }
    @Override
    public void configureOutputJobProperties(TableDesc tableDesc,
            Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        copyJobProperties(properties, jobProperties);
    }

    /*
     * Helper function to copy properties
     */
    private void copyJobProperties(Properties from, Map<String, String> to) {
        for (Entry<Object, Object> e : from.entrySet()) {
            to.put((String)e.getKey(), (String)e.getValue());
        }
    }
}
