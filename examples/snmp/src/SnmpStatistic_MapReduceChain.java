// SnmpStatistic_MapReduceChain.java
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

package com.mongodb.hadoop.examples;

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
 
public class SnmpStatistic_MapReduceChain extends MongoTool {
   public static class MapHostUploadEachAPEachDay extends Mapper<Object, BSONObject, Text, LongWritable> {
      private final static IntWritable one = new IntWritable( 1 );
      private final Text word = new Text();
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
         context.write(key, new LongWritable(totalOutput));
      }
   }    

   public static class MapHostUploadEachDay extends Mapper<Object, BSONObject, Text, LongWritable> {
      @Override
      public void map( Object key , BSONObject value , Context context)throws IOException, InterruptedException {
      //Identity mapper
         String outputKey = new String( value.get( "_id" ).toString() );
         String [] valueItem = outputKey.split( "," );
         String macAdd = valueItem[1];
         LongWritable outputValue = new LongWritable( Long.parseLong( value.get( "value" ).toString() ) );
	     context.write( new Text( macAdd ), outputValue );
	  }
   }

   public static class ReduceHostUploadEachDay extends Reducer<Text, LongWritable, Text, LongWritable> {
      @Override
      public void reduce( Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
         Long totalUploadFlow = new Long(0);
         for ( LongWritable val : values)
           totalUploadFlow += val.get();
         context.write(key, new LongWritable(totalUploadFlow));		        
	  } 
   }
    
   @Override
   public int run( String[] args ) throws Exception {
      final Configuration conf = getConf();
      final com.mongodb.MongoURI outputUri =  MongoConfigUtil.getOutputURI( conf );
      if ( outputUri == null )
        throw new IllegalStateException( "output uri is not set" );
      if ( MongoConfigUtil.getInputURI( conf ) == null )
        throw new IllegalStateException( "input uri is not set" );
      final String outputCollectionName = outputUri.getCollection();
      if ( !outputCollectionName.startsWith( "second" ) ) {
        final Job job = new Job( conf, "snmp analysis " + outputCollectionName );
        job.setJarByClass( SnmpStatistic_MapReduceChain.class );
        job.setMapperClass( MapHostUploadEachAPEachDay.class );
        job.setReducerClass( ReduceHostUploadEachAPEachDay.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( LongWritable.class );
        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );
        boolean result = job.waitForCompletion( true );
        return ( result ? 0:1 );
      } else {
        final Job secondJob = new Job( conf , "snmp analysis "+ outputCollectionName );
        secondJob.setJarByClass( SnmpStatistic_MapReduceChain.class );
        secondJob.setMapperClass( MapHostUploadEachDay.class );
        secondJob.setReducerClass( ReduceHostUploadEachDay.class );
        secondJob.setOutputKeyClass( Text.class );
        secondJob.setOutputValueClass( LongWritable.class );
        secondJob.setInputFormatClass( MongoInputFormat.class );
        secondJob.setOutputFormatClass( MongoOutputFormat.class );
        boolean result2 = secondJob.waitForCompletion( true );
        return ( result2 ? 0:1 );
      }
   }

   //In the test case, we need to design a MapReduce chain to get our result. 
   public static void main( String[] args ) throws Exception{
      boolean use_shards=true;
      boolean use_chunks=false;
      //******************This is the first job.******************/
      final Configuration firstConf = new Configuration();
      MongoConfigUtil.setInputURI( firstConf, "mongodb://localhost:30000/test.snmp" );
      firstConf.setBoolean( MongoConfigUtil.SPLITS_USE_SHARDS, use_shards );
      firstConf.setBoolean( MongoConfigUtil.SPLITS_USE_CHUNKS, use_chunks );
      String output_table = null;
      if ( use_chunks ){
        if( use_shards )
          output_table = "snmp_with_shards_and_chunks";
        else
          output_table = "snmp_with_chunks";
      } else {
        if( use_shards )
          output_table = "snmp_with_shards";
        else
          output_table = "snmp_no_splits";
      }
      MongoConfigUtil.setOutputURI( firstConf, "mongodb://localhost:30000/test." + output_table );
      final Job firstJob = new Job( firstConf , "snmp analysis "+output_table );        
      firstJob.setJarByClass( SnmpStatistic_MapReduceChain.class );
      firstJob.setMapperClass( MapHostUploadEachAPEachDay.class );
      firstJob.setReducerClass( ReduceHostUploadEachAPEachDay.class );
      firstJob.setOutputKeyClass( Text.class );
      firstJob.setOutputValueClass( LongWritable.class ); 
      firstJob.setInputFormatClass( MongoInputFormat.class );
      firstJob.setOutputFormatClass( MongoOutputFormat.class );
      try {
        boolean result = firstJob.waitForCompletion( true );
        System.out.println( "job.waitForCompletion( true ) returned " + result );
      } catch(Exception e) {
        System.out.println( "job.waitForCompletion( true ) threw Exception" );
        e.printStackTrace();
      }
      
      //*****************This is the second job.********************/       
      final Configuration secondConf = new Configuration();
      MongoConfigUtil.setInputURI( secondConf, "mongodb://localhost:30000/test."+ output_table );
      secondConf.setBoolean( MongoConfigUtil.SPLITS_USE_SHARDS, use_shards );
      secondConf.setBoolean( MongoConfigUtil.SPLITS_USE_CHUNKS, use_chunks );
      String output_table2 = null;
      if ( use_chunks ) {
        if( use_shards )
          output_table2 = "second_snmp_with_shards_and_chunks";
        else
          output_table2 = "second_snmp_with_chunks";
      } else {
        if ( use_shards )
          output_table2 = "second_snmp_with_shards";
        else
          output_table2 = "second_snmp_no_splits";
      }
      MongoConfigUtil.setOutputURI( secondConf, "mongodb://localhost:30000/test." + output_table2 );
      final Job secondJob = new Job( secondConf , "snmp analysis "+ output_table2 );        
      secondJob.setJarByClass( SnmpStatistic_MapReduceChain.class );
      secondJob.setMapperClass( MapHostUploadEachDay.class );
      secondJob.setReducerClass( ReduceHostUploadEachDay.class );
      secondJob.setOutputKeyClass( Text.class );
      secondJob.setOutputValueClass( LongWritable.class ); 
      secondJob.setInputFormatClass( MongoInputFormat.class );
      secondJob.setOutputFormatClass( MongoOutputFormat.class );
      try{
        boolean result2 = secondJob.waitForCompletion( true );
        System.out.println( "job.waitForCompletion( true ) returned "+ result2 );
      }catch(Exception e){
        System.out.println( "job.waitForCompletion( true ) threw Exception" );
        e.printStackTrace();
      }
   }
}
