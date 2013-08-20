/*
 * Copyright 2010-2013 10gen Inc.
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
