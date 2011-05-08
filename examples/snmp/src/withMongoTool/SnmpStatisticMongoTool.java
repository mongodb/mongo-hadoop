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
                    else
                        System.out.println("The length of item is less than 5! The value is "+content);
                  }

            }
            else 
                System.out.println("The value.get(\"key\") is null. The key is " + key);
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
      job.setJarByClass(SnmpStatistic.class);
      job.setMapperClass(MapHostUploadEachAPEachDay.class);
      job.setReducerClass(ReduceHostUploadEachAPEachDay.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);
      job.setInputFormatClass(MongoInputFormat.class);
      job.setOutputFormatClass(MongoOutputFormat.class);

      final long start = System.currentTimeMillis();
      System.out.println(" ----------------------- running test "+ outputCollectionName +" --------------------");
      boolean result = job.waitForCompletion( true );
      System.out.println("job.waitForCompletion( true ) returned " + result);
      final long end = System.currentTimeMillis();
      final float seconds = ((float)(end - start))/1000;
      java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
      nf.setMaximumFractionDigits(3);
      System.out.println("finished run in "+ nf.format(seconds) +" seconds");
      return (result?0:1);
      
    }

    private final static void test(boolean use_shards, boolean use_chunks) throws Exception{
        did_start = false;
        //In the test case, we need to design a MapReduce chain to get our result. 

        //******************This is the first job.******************/

        System.out.println(" ----------------------- First MapReduce Job Starts --------------------");
        final Configuration conf1 = new Configuration();
        MongoConfigUtil.setInputURI( conf1, "mongodb://localhost:30000/test.snmp" );
        conf1.setBoolean(MongoConfigUtil.SPLITS_USE_SHARDS, use_shards);
        conf1.setBoolean(MongoConfigUtil.SPLITS_USE_CHUNKS, use_chunks);
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
        MongoConfigUtil.setOutputURI( conf1, "mongodb://localhost:30000/test." + output_table );
        System.out.println( "Conf: " + conf1 );
        
        final Job job = new Job( conf1 , "snmp analysis "+output_table );        
        job.setJarByClass( SnmpStatistic.class );
        job.setMapperClass( MapHostUploadEachAPEachDay.class );
        job.setReducerClass( ReduceHostUploadEachAPEachDay.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( LongWritable.class ); 
        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );

        final long start = System.currentTimeMillis();
        System.out.println(" ----------------------- running test "+ output_table +" --------------------");
        boolean result = job.waitForCompletion( true );
        System.out.println("job.waitForCompletion( true ) returned " + result);
        final long end = System.currentTimeMillis();
        final float seconds = ((float)(end - start))/1000;
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setMaximumFractionDigits(3);
        System.out.println("finished run in "+ nf.format(seconds) +" seconds");
        System.exit(result?0:1);
    }
 
    public static void main( String[] args ) throws Exception{
        //boolean[] tf = {false, true};
        //for(boolean use_shards : tf)
        //    for(boolean use_chunks : tf)
        boolean use_shards=true;
        boolean use_chunks=false;
                test(use_shards, use_chunks);
    }
}
