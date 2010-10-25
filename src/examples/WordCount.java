package com.mongodb.hadoop.examples;
import java.io.*;
import java.util.*;

import org.apache.hadoop.util.ToolRunner;

import org.bson.*;
import com.mongodb.*;
import com.mongodb.hadoop.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.util.MongoTool;

// WordCount.java


/**
 * test.in
 db.in.insert( { x : "eliot was here" } )
 db.in.insert( { x : "eliot is here" } )
 db.in.insert( { x : "who is here" } )
  =
 */
public class WordCount extends MongoTool {


    public static class TokenizerMapper extends Mapper<Object, BSONObject, Text, IntWritable>{
      
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
      
      public void map(Object key, BSONObject value, Context context ) 
          throws IOException, InterruptedException {
          
          System.out.println( "key: " + key );
          System.out.println( "value: " + value );

          StringTokenizer itr = new StringTokenizer(value.get( "x" ).toString());
          while (itr.hasMoreTokens()) {
              word.set(itr.nextToken());
              context.write(word, one);
          }
      }
    }
    
    
    public static class IntSumReducer  extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values,  Context context ) 
            throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    static {
        // Load the XML config defined in hadoop-local.xml
        Configuration.addDefaultResource("src/examples/hadoop-local.xml");
        Configuration.addDefaultResource("src/examples/mongo-defaults.xml");
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }
}
