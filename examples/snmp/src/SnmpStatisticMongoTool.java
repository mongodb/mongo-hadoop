// SnmpStatisticMongoTool.java
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
//package com.mongodb.hadoop.examples;

import java.io.*;
import java.util.*;
import java.lang.String; 
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;
import java.util.ArrayList;
 
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;
 
public class SnmpStatisticMongoTool extends MongoTool{
   public static class MapHostUploadEachAPEachDay extends Mapper<Object, BSONObject, Text, LongWritable> {
      @Override
      public void map( Object key , BSONObject value , Context context ) throws IOException, InterruptedException{
         if (value.get("key") != null) {
           String inputKey = value.get("key").toString();
           if(inputKey.equals("c2")) {
             String content = value.get( "content" ).toString() ;
             String date = value.get( "date" ).toString();
             String [] item = content.split(",");
             int strlength = item.length;
             if (strlength > 5) {
               String apID = item[2];
               String macAdd = item[3];
               String outputFlow = item[5]; // Number of observed octets for which this entry was the source.
               String keyString = date + "," + macAdd + "," + apID;  //Get middle input key for reducer.
               LongWritable valueLong = new LongWritable(Long.parseLong(outputFlow));
               context.write(new Text(keyString), valueLong);
             }
           }
         }
      }
   }
 
   public static class ReduceHostUploadEachAPEachDay extends Reducer<Text, LongWritable, Text, LongWritable> {
      @Override
      public void reduce( Text key , Iterable<LongWritable> values , Context context ) throws IOException, InterruptedException{
         ArrayList<Long> outputFlowArray = new ArrayList<Long>();
         for (LongWritable val : values) {
	       outputFlowArray.add(val.get());
         }
         Long totalOutput = Collections.max(outputFlowArray) - Collections.min(outputFlowArray);
         String reduceInputKey=key.toString(); 
         String [] item = reduceInputKey.split(",");
         String date = item[0];
         String macAdd = item[1];
         String apID = item[2];
         String outputKey = date + "," + macAdd;
         context.write(new Text(outputKey), new LongWritable(totalOutput));
      }
   }

   //run() method for test harness.
   @Override
   public int run(String[] args) throws Exception {
      final Configuration conf = getConf();
      final com.mongodb.MongoURI outputUri =  MongoConfigUtil.getOutputURI(conf);
      if (outputUri == null)
        throw new IllegalStateException("output uri is not set");
      if (MongoConfigUtil.getInputURI(conf) == null)
        throw new IllegalStateException("input uri is not set");
      final String outputCollectionName = outputUri.getCollection();
      final Job job = new Job(conf, "snmp analysis " + outputCollectionName);
      job.setJarByClass(SnmpStatisticMongoTool.class);
      job.setMapperClass(MapHostUploadEachAPEachDay.class);
      job.setReducerClass(ReduceHostUploadEachAPEachDay.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);
      job.setInputFormatClass(MongoInputFormat.class);
      job.setOutputFormatClass(MongoOutputFormat.class);

      boolean result = job.waitForCompletion( true );
      return (result?0:1);   
    }
 
    public static void main( String[] args ) throws Exception{
	    boolean use_shards=true;
        boolean use_chunks=false;
	   	final Configuration conf = new Configuration();
        String output_table = null;

        MongoConfigUtil.setInputURI( conf, "mongodb://localhost:30000/test.snmp" );
        conf.setBoolean(MongoConfigUtil.SPLITS_USE_SHARDS, use_shards);
        conf.setBoolean(MongoConfigUtil.SPLITS_USE_CHUNKS, use_chunks);        
        if (use_chunks){
            if(use_shards)
                output_table = "snmp_with_shards_and_chunks";
            else
                output_table = "snmp_with_chunks";
        }else{
            if(use_shards)
                output_table = "snmp_with_shards";
            else
                output_table = "snmp_no_splits";
        }
        MongoConfigUtil.setOutputURI( conf, "mongodb://localhost:30000/test." + output_table );
        final Job job = new Job( conf , "snmp analysis "+output_table );        
        job.setJarByClass( SnmpStatisticMongoTool.class );
        job.setMapperClass( MapHostUploadEachAPEachDay.class );
        job.setReducerClass( ReduceHostUploadEachAPEachDay.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( LongWritable.class ); 
        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );
      
        boolean result = job.waitForCompletion( true );
        System.exit(result?0:1);
    }
}
