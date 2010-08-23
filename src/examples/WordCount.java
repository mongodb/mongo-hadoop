// WordCount.java

import java.io.*;
import java.util.*;

import org.bson.*;
import com.mongodb.*;
import com.mongodb.hadoop.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class WordCount {


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
    
    public static void main(String[] args) 
        throws Exception {
        
        Configuration conf = new Configuration();
        MongoConfig.setInput( conf , "test" , "in" );
        MongoConfig.setOutput( conf , "test" , "out2" );
        
        Job job = new Job(conf, "word count");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
