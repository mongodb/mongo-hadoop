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

package com.mongodb.hadoop.hive.output;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

/**
 * 
 * A FileOutputFormat for the Storage Handler for renaming the 
 * fileOutputPath to the correct location
 */
@SuppressWarnings("deprecation")
public class HiveBSONStorageHandlerFileOutputFormat<K, V> 
            extends HiveBSONFileOutputFormat<K, V>{
    
       
    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, 
            Path fileOutputPath,
            Class<? extends Writable> valueClass, 
            boolean isCompressed, 
            Properties tableProperties,
            Progressable progress) throws IOException {
        
        return super.getHiveRecordWriter(jc, fileOutputPath, valueClass,
                isCompressed, tableProperties, progress, true);
    }
}
