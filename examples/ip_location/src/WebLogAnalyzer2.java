
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;


import org.apache.hadoop.mapreduce.Mapper.Context;


import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
/**
 * <p>This program analyzes apache web log entries based on their location.</p>
 * <p>Hadoop is supposed to have built in functionality to get data from two
 * sources and to join data based on join keys, but I could not get this to work.
 * Instead a temporary collection is used to join the data.  The location information
 * is stored back in the original input collection for future use.</p>
 * 
 * Created: Apr 11, 2011  1:27:14 PM
 *
 * @author Joseph Shraibman
 */
public class WebLogAnalyzer2 extends MongoTool{
    private final static String BITS_NOW_PROC_NAME = "WebLogAnalyzer2.bitsnow";
    private final static String IP_RANGE_FIELD_NAME = "iprange";
    private final static String WRAPPED_ID_FIELD_NAME = "wrappedid";
    private final static String IpDbFilename = "IpToCountry.csv";
    private static final Log log = LogFactory.getLog( WebLogAnalyzer2.class );
    
    /** Mapper to get input from the ip range database file. */
    public static class IpRangeMapper 
        extends Mapper<LongWritable, Text, Text, BSONWritable> {
          
        private Text rangeText = new Text(); 
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String valStr = value.toString().trim();
            if (valStr.length() == 0 || valStr.charAt(0) == '#')
                return;
            final Map<IpCsvParser.Key, String> map1 = IpCsvParser.parseLine(value.toString());
            if (map1 == null){
                log.error(this.getClass()+" returned null for: "+value);
                return;
            }
                    
            //turn ip range into text like "1.2.3.4/27"
            String range = IpCsvParser.getIpRange(map1.get(IpCsvParser.Key.IP_FROM), map1.get(IpCsvParser.Key.IP_TO));
            rangeText.set(range);
            Map<String, String> map2 = new HashMap<String, String>();
            for(Map.Entry<?, String> me : map1.entrySet()){
                map2.put(me.getKey().toString(), me.getValue());
            }
            if (map2.isEmpty()){
                log.error(this.getClass()+" map2 is empty! for range "+range);
                return;
            }
                
            context.write(rangeText, new BSONWritable(new BasicBSONObject(map2) ));
        }
    }
    public static class IpRangeReducer extends Reducer<Text, BSONWritable, Text, BSONObject>{

        @Override
        protected void reduce(Text key, Iterable<BSONWritable> values, Context context) throws IOException, InterruptedException {
            for(BSONWritable bw : values){
                BasicBSONObject obj = new BasicBSONObject();
                obj.append("iprange", bw);
                String rangeStr = key.toString();
                int bits = Integer.parseInt(rangeStr.substring(rangeStr.indexOf('/')+1));
                obj.append("bits", bits);
                context.write(key, obj); 
            }
        }
    }
    public static class TheWeglogEntryMapper
        extends Mapper<Object, BSONObject, Text, BSONWritable> {
        private int bitsProcessingNow = 0;
        private Text rangeText = new Text(); 
         
        @Override
        protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
            if (value.containsField(IP_RANGE_FIELD_NAME))
                return;
            String ipString = ((BasicBSONObject)value).getString("ip");
            String maskStr = IpCsvParser.getIpRange(ipString, bitsProcessingNow);
            rangeText.set(maskStr);
            context.write(rangeText, new BSONWritable(new BasicBSONObject("weblog_id", (org.bson.types.ObjectId)  key)));
            //output is: iprange, objid   
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            bitsProcessingNow = context.getConfiguration().getInt(BITS_NOW_PROC_NAME, -1);
        }
    }
    public static class TheWeblogEntryReducer extends Reducer<Text, BSONWritable, String, BSONObject>{

        @Override
        public void reduce( Text key , Iterable<BSONWritable> values , Context context ) throws IOException, InterruptedException{

            BasicDBList list = new BasicDBList();
            for ( final BSONWritable weblogId : values ) {
                list.add(weblogId.get("weblog_id"));
            }
            final BasicDBObject val = new BasicDBObject().append("$pushAll", new BasicDBObject().append("weblogids",list));
            final BasicDBObject wrapper = new BasicDBObject();
            wrapper.put("$query", new BasicDBObject("_id", key.toString()));
            wrapper.put("$value", val);
            context.write("$update", wrapper); //"$update" is a magic string right now
        }
    }
    public static class TheWeglogEntryUpdateMapper  
        extends Mapper<Object, BSONObject, BSONWritable, BSONWritable>{

        @Override
        protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
            if (! value.containsField("iprange"))
                return;
            List weblogIdList = (List) value.get("weblogids");
            if (weblogIdList == null)
                return;
            for (Object weblog_id : weblogIdList) {
                //output: weblog _id, iprange
                org.bson.types.ObjectId id =  (org.bson.types.ObjectId)weblog_id;
                BasicDBObject id_wrapper = new BasicDBObject(WRAPPED_ID_FIELD_NAME, id);
                context.write( new BSONWritable(id_wrapper), new BSONWritable((BSONObject)value.get("iprange")));
            }
        }
    }
    public static class TheWeglogEntryUpdateReducer extends Reducer<BSONWritable, BSONWritable, String, BSONObject>{
        @Override
        protected void reduce(BSONWritable wrapped_id, Iterable<BSONWritable> values, Context context) throws IOException, InterruptedException {
            BasicDBObject query = new BasicDBObject("_id", wrapped_id.get(WRAPPED_ID_FIELD_NAME));
             
            BasicDBObject value = new BasicDBObject( values.iterator().next().toMap());
            final BasicDBObject val = new BasicDBObject().append("$set",new BasicDBObject(IP_RANGE_FIELD_NAME,value));
            final BasicDBObject wrapper = new BasicDBObject();
            wrapper.put("$query", query);
            wrapper.put("$value", val);
            context.write( "$update", wrapper);
        }
    }
  
    public static class TheWeglogEntryAnalMapper 
        extends Mapper<Object, BSONObject, Text, IntWritable> 
    {
        private Text keyText = new Text();
        private final IntWritable One = new IntWritable(1);
        @Override
        protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
            if (! value.containsField(IP_RANGE_FIELD_NAME))
                return;
            final BasicBSONObject IpRangeObj = (BasicBSONObject) value.get(IP_RANGE_FIELD_NAME);
            if (IpRangeObj == null)
                return;
            String country = IpRangeObj.getString("COUNTRY");
            if (country == null)
                return;
            keyText.set(country);
            context.write(keyText, One);
        }
       
    }
    public static class TheWeglogEntryAnalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
    
    private boolean runIpRangePhase(com.mongodb.MongoURI tempCollUri)throws Exception{
        final Configuration originalConf = getConf();
        
        final Configuration stage1Conf = new Configuration(originalConf);
        MongoConfigUtil.setOutputURI(stage1Conf, tempCollUri);
        Job job = new Job(stage1Conf);
        job.setJarByClass(this.getClass());
        job.setJobName("ip range phase");
        
        job.setOutputFormatClass( MongoOutputFormat.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( BSONObject.class );
        job.setMapperClass( IpRangeMapper.class );
        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( BSONWritable.class );
        job.setReducerClass( IpRangeReducer.class );
        if (job.getMapOutputValueClass() == null)
            throw new RuntimeException("How come job.getMapOutputValueClass() == null ?");
        
        job.setInputFormatClass( TextInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );
        
        FileInputFormat.setInputPaths(job, new Path(IpDbFilename));
        
        return job.waitForCompletion(true);
    }
    private boolean runWeblogInputPhase(com.mongodb.MongoURI tempCollUri, int bits)throws Exception{
        final Configuration originalConf = getConf();
        
        final Configuration thisStageConf = new Configuration(originalConf);
        MongoConfigUtil.setOutputURI(thisStageConf, tempCollUri);
        //query for entries that do not have range set yet
        MongoConfigUtil.setQuery(thisStageConf, "{\""+IP_RANGE_FIELD_NAME+"\" : { \"$exists\" : false}}");
        thisStageConf.setInt(BITS_NOW_PROC_NAME, bits);
        Job job = new Job(thisStageConf);
        job.setJarByClass(this.getClass());
        job.setJobName("weblog input phase");
        
        job.setOutputFormatClass( MongoOutputFormat.class );
        job.setOutputKeyClass( String.class );
        job.setOutputValueClass( BSONObject.class );
        job.setMapperClass( TheWeglogEntryMapper.class );
        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( BSONWritable.class );
        job.setReducerClass( TheWeblogEntryReducer.class );
        
        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );
        
        return job.waitForCompletion(true);
    }
    private boolean runUpdateOriginalEntriesPhase(com.mongodb.MongoURI tempCollUri, int bits)throws Exception{
        final Configuration originalConf = getConf();
        
        final Configuration thisStageConf = new Configuration(originalConf);
        MongoConfigUtil.setOutputURI(thisStageConf, MongoConfigUtil.getInputURI(originalConf));
        MongoConfigUtil.setInputURI(thisStageConf, tempCollUri);
        //for entries in the temp table that do match an ip range with data
        //start process to update data in original table
        MongoConfigUtil.setQuery(thisStageConf, "{\"iprange\":{\"$exists\":true}, \"bits\":"+bits+"}");
        thisStageConf.setInt(BITS_NOW_PROC_NAME, bits);
        Job job = new Job(thisStageConf);
        job.setJarByClass(this.getClass());
        job.setJobName("update weblog phase bits "+bits);
        
        job.setOutputFormatClass( MongoOutputFormat.class );
        job.setOutputKeyClass( String.class );
        job.setOutputValueClass( BSONObject.class );
        job.setMapperClass( TheWeglogEntryUpdateMapper.class );
        job.setMapOutputKeyClass( BSONWritable.class );
        job.setMapOutputValueClass( BSONWritable.class );
        job.setReducerClass( TheWeglogEntryUpdateReducer.class );
        
        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );
        
        return job.waitForCompletion(true);
    }
    private boolean runAnalPhase()throws Exception{
        final Configuration originalConf = getConf();
        
        final Configuration stage1Conf = new Configuration(originalConf);
        Job job = new Job(stage1Conf);
        job.setJarByClass(this.getClass());
        job.setJobName("weblog anal phase");
        
        job.setMapperClass( TheWeglogEntryAnalMapper.class );

        job.setCombinerClass( TheWeglogEntryAnalReducer.class );
        job.setReducerClass( TheWeglogEntryAnalReducer.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );
        
        return job.waitForCompletion(true);
    }
      
    /** Get a new URI that is the same as the old URI except the collection is different */
    private static com.mongodb.MongoURI getNewUri(com.mongodb.MongoURI uri, String newCollectionName){
        StringBuilder sb = new StringBuilder(uri.toString());
        int slashIdx = sb.indexOf("/");
        int periodIdx = sb.indexOf(".", slashIdx);
        int qIdx = sb.indexOf("?", periodIdx);
        if (qIdx > 0){
            sb.replace(periodIdx+1, qIdx, newCollectionName);
            return new com.mongodb.MongoURI(sb.toString());
        }
        sb.replace(periodIdx+1, sb.length(), newCollectionName);
        return new com.mongodb.MongoURI(sb.toString());       
    }
    private void addIndex(com.mongodb.MongoURI uri){
        Mongo mongo = null;
        try{
            mongo = uri.connect();
            final DBCollection coll = uri.connectCollection(mongo);
            coll.createIndex(new BasicDBObject("bits", 1));
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if (mongo != null)
                mongo.close();
        }
    }
    static void shardCollection(com.mongodb.MongoURI uri){
        Mongo mongo = null;
        try{
            mongo = uri.connect();
            DB adminDb  = mongo.getDB("admin");
            BasicDBObject command = new BasicDBObject("shardcollection", uri.getDatabase()+"."+uri.getCollection() ).
                append("key" , new BasicDBObject("_id", 1)); 
            final CommandResult commandResult = adminDb.command(command);
            log.info("running shard command result: "+commandResult);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if (mongo != null)
                mongo.close();
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        final Configuration originalConf = getConf();
        originalConf.set("mapred.job.tracker", "local");
        if (originalConf == null)
            throw new java.lang.IllegalStateException("getConf() returned null!");
        if (MongoConfigUtil.getInputURI(originalConf) == null)
            throw new java.lang.IllegalStateException("input uri is not configured");
            
        
        final String workingCollectionName = "tempColl"+System.currentTimeMillis();
        final com.mongodb.MongoURI originalOutputUri =  MongoConfigUtil.getOutputURI( originalConf);
        if (originalOutputUri == null)
            throw new java.lang.IllegalStateException("output uri is not configured");

        //If the data to be processed is very large a temp database can be used instead of a temp collection.
        //That way the added documents will be at the top level and can be spread among the shards.
        final com.mongodb.MongoURI tempCollectionUri = getNewUri(originalOutputUri, workingCollectionName);
        log.debug("temp collection uri: "+tempCollectionUri);
        shardCollection(tempCollectionUri);
        addIndex(tempCollectionUri);
        
        runIpRangePhase(tempCollectionUri); //get ip data into the temp collection for joining
        
        for(int bits = 29 ; bits >= 1 ; bits--){
            System.out.println("---------------- running for bits "+bits+"-----------------------");
            if (!runWeblogInputPhase(tempCollectionUri, bits)) //write objids to temp table
                return 1; 
            System.out.println("    ---------- updating original entries for bits "+bits+"--------");
            if (!runUpdateOriginalEntriesPhase(tempCollectionUri, bits))
                return 2;
            
            //FUTURE: save ip address along with _id in runWeblogInputPhase()
            //  Then delete entries in temp table where bits = bits and range is set,
            //  The remainder can we used as input for the next round
            
            //now delete entries we don't need anymore
            Mongo mongo = tempCollectionUri.connect();
            try{
                final DBCollection tempCollection = tempCollectionUri.connectCollection(mongo);
                final WriteResult removeResult = tempCollection.remove(new BasicDBObject("bits", bits));
                try{
                    System.out.println("removing entries of bits "+bits+" returned result "+removeResult);
                }catch(Exception e){
                    e.printStackTrace();
                }
                if (bits == 1)  //last loop iteration, take advantage of open connection and drop collection here
                    tempCollection.drop();
            }finally{
                if (mongo != null)
                    mongo.close();
            }
        }
        
        System.out.println("running analysis phase");
        return runAnalPhase() ? 0 : 1;
    }
    public static final void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI( conf, "mongodb://localhost:30010/test.weblog" );
        MongoConfigUtil.setOutputURI( conf, "mongodb://localhost:30010/test.weblogAnalOutput" );
        ToolRunner.run(conf,new WebLogAnalyzer2(), args);
    }
}
