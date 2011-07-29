// SnmpStatisticWithCombiner.java
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

public class SnmpStatisticWithCombiner extends MongoTool {
    public static class MapHostUploadOnEachAPPerDay extends Mapper<Object, BSONObject, Text, LongWritable> {
        private final static IntWritable one = new IntWritable( 1 );
        private final Text word = new Text();

        @Override
        public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{
            if ( value.get( "key" ) != null ){
                String inputKey = value.get( "key" ).toString();
                if ( inputKey.equals( "c2" ) ){
                    String content = value.get( "content" ).toString();
                    String date = value.get( "date" ).toString();
                    String[] item = content.split( "," );
                    int strlength = item.length;
                    if ( strlength > 5 ){
                        String apID = item[2];
                        String macAdd = item[3];
                        String outputFlow = item[5]; // Number of observed octets for which this entry was the source.
                        String keyString = date + "," + macAdd + "," + apID;  //Get middle input key for reducer.
                        LongWritable valueLong = new LongWritable( Long.parseLong( outputFlow ) );
                        context.write( new Text( keyString ), valueLong );
                    }
                }
            }
        }
    }

    public static class CombineHostUploadOnEachAPPerDay extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce( Text key, Iterable<LongWritable> values, Context context )
                throws IOException, InterruptedException{
            ArrayList<Long> outputFlowArray = new ArrayList<Long>();
            for ( LongWritable val : values ){
                outputFlowArray.add( val.get() );
            }
            Long totalOutput = Collections.max( outputFlowArray ) - Collections.min( outputFlowArray );
            String combinerInputKey = key.toString();
            String[] item = combinerInputKey.split( "," );
            String date = item[0];
            String macAdd = item[1];
            String outputKey = date + "," + macAdd;
            context.write( new Text( outputKey ), new LongWritable( totalOutput ) );
        }
    }

    public static class ReduceHostUploadOnEachAPPerDay extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce( Text key, Iterable<LongWritable> values, Context context )
                throws IOException, InterruptedException{
            Long totalUploadFlow = new Long( 0 );
            for ( LongWritable val : values ){
                totalUploadFlow += val.get();
            }
            context.write( key, new LongWritable( totalUploadFlow ) );
        }
    }

    @Override
    public int run( String[] args ) throws Exception{
        final Configuration conf = getConf();
        final com.mongodb.MongoURI outputUri = MongoConfigUtil.getOutputURI( conf );
        if ( outputUri == null )
            throw new IllegalStateException( "output uri is not set" );
        if ( MongoConfigUtil.getInputURI( conf ) == null )
            throw new IllegalStateException( "input uri is not set" );
        final String outputCollectionName = outputUri.getCollection();
        final Job job = new Job( conf, "snmp analysis " + outputCollectionName );
        job.setJarByClass( SnmpStatisticWithCombiner.class );
        job.setMapperClass( MapHostUploadOnEachAPPerDay.class );
        job.setCombinerClass( CombineHostUploadOnEachAPPerDay.class );
        job.setReducerClass( ReduceHostUploadOnEachAPPerDay.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( LongWritable.class );
        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );
        try {
            boolean result = job.waitForCompletion( true );
            System.out.println( "job.waitForCompletion( true ) returned " + result );
        }
        catch ( Exception e ) {
            System.out.println( "job.waitForCompletion( true ) threw Exception" );
            e.printStackTrace();
        }
        return 0;
    }

    public static void main( String[] args ) throws Exception{
        boolean use_shards = true;
        boolean use_chunks = false;
        final Configuration Conf = new Configuration();
        MongoConfigUtil.setInputURI( Conf, "mongodb://localhost:30000/test.snmp" );
        Conf.setBoolean( MongoConfigUtil.SPLITS_USE_SHARDS, use_shards );
        Conf.setBoolean( MongoConfigUtil.SPLITS_USE_CHUNKS, use_chunks );
        String output_table = null;
        if ( use_chunks ){
            if ( use_shards )
                output_table = "snmp_with_shards_and_chunks";
            else
                output_table = "snmp_with_chunks";
        }
        else{
            if ( use_shards )
                output_table = "snmpWithShards";
            else
                output_table = "snmp_no_splits";
        }
        MongoConfigUtil.setOutputURI( Conf, "mongodb://localhost:30000/test." + output_table );
        final Job snmpJob = new Job( Conf, "snmp analysis " + output_table );
        snmpJob.setJarByClass( SnmpStatisticWithCombiner.class );
        snmpJob.setMapperClass( MapHostUploadOnEachAPPerDay.class );
        snmpJob.setCombinerClass( CombineHostUploadOnEachAPPerDay.class );
        snmpJob.setReducerClass( ReduceHostUploadOnEachAPPerDay.class );
        snmpJob.setOutputKeyClass( Text.class );
        snmpJob.setOutputValueClass( LongWritable.class );
        snmpJob.setInputFormatClass( MongoInputFormat.class );
        snmpJob.setOutputFormatClass( MongoOutputFormat.class );
        try {
            boolean result = snmpJob.waitForCompletion( true );
            System.out.println( "job.waitForCompletion( true ) returned " + result );
        }
        catch ( Exception e ) {
            System.out.println( "job.waitForCompletion( true ) threw Exception" );
            e.printStackTrace();
        }
    }
}
