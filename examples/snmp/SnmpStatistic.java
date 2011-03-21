import java.io.*;
import java.util.*;
 
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;
 
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;
 
public class SnmpStatistic {
 
   private static boolean did_start = false;
 
    public static class MapHostUploadEachAPEachDay extends Mapper<Object, BSONObject, Text, LongWritable> {
 
        private final static IntWritable one = new IntWritable( 1 );
        private final Text word = new Text();
        @Override
        public void map( Object key , BSONObject value , Context context ) throws IOException, InterruptedException{
            if (! did_start){
                System.out.println( "map starting, config: "+ context.getConfiguration());
                did_start = true;
            }
 
            //System.out.println( "key: " + key );
            //System.out.println( "value: " + value );
            //System.out.println(value.get("key").toString());
            String inputKey = value.get("key").toString();
            if(inputKey.equals("c2")) {
	            String content = value.get( "content" ).toString() ;
	            String date = value.get( "date" ).toString();
	            String [] item = content.split(",");
	            String apID = item[2];
	            String macAdd = item[3];
	            String outputFlow = item[5]; // Number of observed octets for which this entry was the source.
	            String keyString = date + "," + macAdd + "," + apID;  //Get middle input key for reducer.
	            //System.out.println("Map output key is "+keyString);
                    
                    LongWritable valueLong = new LongWritable(Long.parseLong(outputFlow));
	           // System.out.println("Map output value is "+valueLong.get());
                    context.write(new Text(keyString), valueLong);
            }
            //else {
            //        System.out.println("The key is "+inputKey+", but c2");
            //}            
        }
    }
 
    public static class ReduceHostUploadEachAPEachDay extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce( Text key , Iterable<LongWritable> values , Context context ) throws IOException, InterruptedException{
 
            Vector<Long> outputFlow_vector = new Vector<Long>();
            for (LongWritable val : values) {
	             outputFlow_vector.add(val.get());
            }
            Long totalOutput = Collections.max(outputFlow_vector) - Collections.min(outputFlow_vector);
            System.out.println("Input reduce key is "+key.toString());
            String reduceInputKey=key.toString(); 
            String [] item = reduceInputKey.split(",");
            System.out.println("Item[0] is "+item[0]+"; Item[1] is "+item[1]+"; Item[2] is "+item[2]);
            String date = item[0];
            String macAdd = item[1];
            String apID = item[2];
            String outputKey = date + "," + macAdd;
            //System.out.println("Reduce output key is "+outputKey);
            //System.out.println("Reduce output value is "+totalOutput.toString());
            context.write(new Text(outputKey), new LongWritable(totalOutput));
        }
    }    

    public static class MapHostUploadEachDay extends Mapper<LongWritable, Text, Text, LongWritable> {
	     @Override
	     public void map( LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		    //Identity mapper
		    String [] item = value.toString().split("	");
		    String outputKey = item[0];
		    LongWritable outputValue = new LongWritable(Long.parseLong(item[1].toString()));
		    context.write(new Text(outputKey), outputValue);
	     }
    }

    public static class ReduceHostUploadEachDay extends Reducer<Text, LongWritable, Text, LongWritable> {
	     @Override
	     public void reduce( Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		        Long totalUploadFlow = new Long(0);
		        for ( LongWritable val : values){
			         totalUploadFlow += val.get();
		        }
		        context.write(key, new LongWritable(totalUploadFlow));		        
	     } 
    }
    
    private final static void test(boolean use_shards, boolean use_chunks) throws Exception{
        did_start = false;
        //In the test case, we need to design a MapReduce chain to get our result. 

        //******************This is the first job.******************/
        System.out.println(" ----------------------- First MapReduce Job Starts --------------------");
        final Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI( conf, "mongodb://localhost:30000/test.snmp" );
        conf.setBoolean(MongoConfigUtil.SPLITS_USE_SHARDS, use_shards);
        conf.setBoolean(MongoConfigUtil.SPLITS_USE_CHUNKS, use_chunks);
        String output_table = null;
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
        System.out.println( "Conf: " + conf );
        
        final Job job = new Job( conf , "snmp analysis "+output_table );        
        job.setJarByClass( SnmpStatistic.class );
        job.setMapperClass( MapHostUploadEachAPEachDay.class );
        job.setReducerClass( ReduceHostUploadEachAPEachDay.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( LongWritable.class ); 
        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );

        final long start = System.currentTimeMillis();
        System.out.println(" ----------------------- running test "+ output_table +" --------------------");
        try{
            boolean result = job.waitForCompletion( true );
            System.out.println("job.waitForCompletion( true ) returned " + result);
        }catch(Exception e){
            System.out.println("job.waitForCompletion( true ) threw Exception");
            e.printStackTrace();
        }
        final long end = System.currentTimeMillis();
        final float seconds = ((float)(end - start))/1000;
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setMaximumFractionDigits(3);
        System.out.println("finished run in "+ nf.format(seconds) +" seconds");
 
        /*com.mongodb.Mongo m = new com.mongodb.Mongo( new com.mongodb.MongoURI("mongodb://localhost:30000/"));
        com.mongodb.DB db = m.getDB( "test" );
        com.mongodb.DBCollection coll = db.getCollection(output_table);
        com.mongodb.BasicDBObject query = new com.mongodb.BasicDBObject();
        query.put( "_id","");
        com.mongodb.DBCursor cur = coll.find(query);
        if (! cur.hasNext())
            System.out.println("FAILURE: could not find count of \'the\'");
        else
            System.out.println("'the' count: "+cur.next());

        */
         //*****************This is the second job.********************/
/*      The MapReduce chain is not available so far.
        System.out.println(" ----------------------- Second MapReduce Job Starts --------------------");
	    final Configuration conf2 = new Configuration();
        MongoConfigUtil.setInputURI( conf2, "mongodb://localhost:30000/test."+ output_table );
        conf2.setBoolean(MongoConfigUtil.SPLITS_USE_SHARDS, use_shards);
        conf2.setBoolean(MongoConfigUtil.SPLITS_USE_CHUNKS, use_chunks);
        String output_table2 = null;
        if (use_chunks){
            if(use_shards)
                output_table2 = "final_snmp_with_shards_and_chunks";
            else
                output_table2 = "final_snmp_with_chunks";
        }else{
            if(use_shards)
                output_table2 = "final_snmp_with_shards";
            else
                output_table2 = "final_snmp_no_splits";
        }
        MongoConfigUtil.setOutputURI( conf2, "mongodb://localhost:30000/test." + output_table2 );
        System.out.println( "Conf: " + conf2 );
        
        final Job job2 = new Job( conf2 , "snmp analysis "+ output_table2 );        
        job2.setJarByClass( SnmpStatistic.class );
        job2.setMapperClass( MapHostUploadEachDay.class );
        job2.setReducerClass( ReduceHostUploadEachDay.class );
        job2.setOutputKeyClass( Text.class );
        job2.setOutputValueClass( LongWritable.class ); 
        job2.setInputFormatClass( MongoInputFormat.class );
        job2.setOutputFormatClass( MongoOutputFormat.class );

        final long start2 = System.currentTimeMillis();
        System.out.println(" ----------------------- running test "+output_table +" --------------------");
        try{
            boolean result2 = job2.waitForCompletion( true );
            System.out.println("job.waitForCompletion( true ) returned "+ result2);
        }catch(Exception e){
            System.out.println("job.waitForCompletion( true ) threw Exception");
            e.printStackTrace();
        }
        final long end2 = System.currentTimeMillis();
        final float seconds2 = ((float)(end2 - start2))/1000;
        java.text.NumberFormat nf2 = java.text.NumberFormat.getInstance();
        nf2.setMaximumFractionDigits(3);
        System.out.println("finished run in "+nf2.format(seconds2)+" seconds");

        /*com.mongodb.Mongo m2 = new com.mongodb.Mongo( new com.mongodb.MongoURI("mongodb://localhost:30000/"));
        com.mongodb.DB db2 = m2.getDB( "test" );
        com.mongodb.DBCollection coll2 = db2.getCollection(output_table);
        com.mongodb.BasicDBObject query2 = new com.mongodb.BasicDBObject();
        query2.put( "_id","the");
        com.mongodb.DBCursor cur2 = coll.find(query);
        if (! cur2.hasNext())
            System.out.println("FAILURE: could not find count of \'the\'");
        else
            System.out.println("'the' count: "+cur2.next());
        */
    }
 
    public static void main( String[] args ) throws Exception{
        boolean[] tf = {false, true};
        for(boolean use_shards : tf)
            for(boolean use_chunks : tf)
        //boolean use_shards=true;
        //boolean use_chunks=true;
                test(use_shards, use_chunks);
    }
}
