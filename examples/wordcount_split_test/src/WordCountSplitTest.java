/*
 * Copyright 2010 10gen Inc.
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

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;

public class WordCountSplitTest {

    private static final Log log = LogFactory.getLog( WordCountSplitTest.class );
        private static boolean did_start = false;

    public static class TokenizerMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

        private final static IntWritable one = new IntWritable( 1 );
        private final Text word = new Text();


        @Override
        public void map( Object key , BSONObject value , Context context ) throws IOException, InterruptedException{
            if (! did_start){
                System.out.println( "map starting, config: "+context.getConfiguration());
                did_start = true;
            }

//            System.out.println( "key: " + key );
//            System.out.println( "value: " + value );

            String str = value.get( "line" ).toString() ;
            if (str != null){
            final StringTokenizer itr = new StringTokenizer(str );
            while ( itr.hasMoreTokens() ) {
                word.set( itr.nextToken() );
                context.write( word, one );
            }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce( Text key , Iterable<IntWritable> values , Context context ) throws IOException, InterruptedException{

            int sum = 0;
            for ( final IntWritable val : values ) {
                sum += val.get();
            }
            result.set( sum );
            context.write( key, result );
        }
    }
    private final static void test(boolean use_shards, boolean use_chunks, Boolean slaveok) throws Exception{
        did_start = false;
        final Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI( conf, "mongodb://localhost:30000/test.lines" );
        conf.setBoolean(MongoConfigUtil.SPLITS_USE_SHARDS, use_shards);
        conf.setBoolean(MongoConfigUtil.SPLITS_USE_CHUNKS, use_chunks);
        String output_table = null;
        if (use_chunks){
            if(use_shards)
                output_table = "with_shards_and_chunks";
            else
                output_table = "with_chunks";
        }else{
            if(use_shards)
                output_table = "with_shards";
            else
                output_table = "no_splits";
        }
        if (slaveok != null){
            output_table += "_" + slaveok;
        }
        MongoConfigUtil.setOutputURI( conf, "mongodb://localhost:30000/test." +output_table );
        System.out.println( "Conf: " + conf );

        final Job job = new Job( conf , "word count "+output_table );

        job.setJarByClass( WordCountSplitTest.class );

        job.setMapperClass( TokenizerMapper.class );

        job.setCombinerClass( IntSumReducer.class );
        job.setReducerClass( IntSumReducer.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );

        final long start = System.currentTimeMillis();
        System.out.println(" ----------------------- running test "+output_table +" --------------------");
        try{
            boolean result = job.waitForCompletion( true );
            System.out.println("job.waitForCompletion( true ) returned "+result);
        }catch(Exception e){
            System.out.println("job.waitForCompletion( true ) threw Exception");
            e.printStackTrace();
        }
        final long end = System.currentTimeMillis();
        final float seconds = ((float)(end - start))/1000;
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setMaximumFractionDigits(3);
        System.out.println("finished run in "+nf.format(seconds)+" seconds");

        com.mongodb.Mongo m = new com.mongodb.Mongo( new com.mongodb.MongoURI("mongodb://localhost:30000/?slaveok=true"));
        com.mongodb.DB db = m.getDB( "test" );
        com.mongodb.DBCollection coll = db.getCollection(output_table);
        com.mongodb.BasicDBObject query = new com.mongodb.BasicDBObject();
        query.put( "_id","the");
        com.mongodb.DBCursor cur = coll.find(query);
        if (! cur.hasNext())
            System.out.println("FAILURE: could not find count of \'the\'");
        else
            System.out.println("'the' count: "+cur.next());
        
//        if (! result)
//           System.exit(  1 );
    }

    public static void main( String[] args ) throws Exception{
        boolean[] tf = {false, true};
        Boolean[] ntf = {null, Boolean.TRUE, Boolean.FALSE};
        for(boolean use_shards : tf)
            for(boolean use_chunks : tf)
                for(Boolean slaveok : ntf)
                    test(use_shards, use_chunks, slaveok);
    }
}
