
## Example 1 - Treasury Yield Calculation

###Setup

Source code is in `examples/treasury_yield`. To prepare the sample data, first run `mongoimport` with the file in `examples/treasury_yield/src/main/resources/yield_historical_in.json` to load it into a local instance of MongoDB.

We end up with a test collection containing documents that look like this:

    { 
      "_id": ISODate("1990-01-25T19:00:00-0500"), 
      "dayOfWeek": "FRIDAY", "bc3Year": 8.38,
      "bc10Year": 8.49,
      …
    }

###Map/Reduce with Java

The goal is to find the average of the bc10Year field, across each year that exists in the dataset. First we define a mapper, which is executed against each document in the collection. We extract the year from the `_id` field and use it as the output key, along with the value we want to use for averaging, `bc10Year`.

	public class TreasuryYieldMapper 
	    extends Mapper<Object, BSONObject, IntWritable, DoubleWritable> {
	
	    @Override
	    public void map( final Object pKey,
	                     final BSONObject pValue,
	                     final Context pContext )
	            throws IOException, InterruptedException{
	        final int year = ((Date)pValue.get("_id")).getYear() + 1900;
	        double bid10Year = ( (Number) pValue.get( "bc10Year" ) ).doubleValue();
	        pContext.write( new IntWritable( year ), new DoubleWritable( bid10Year ) );
	    }
	}

Then we write a reducer, a function which takes the values collected for each key (the year)  and performs some aggregate computation of them to get a result.

	public class TreasuryYieldReducer
	        extends Reducer<IntWritable, DoubleWritable, IntWritable, BSONWritable> {
	    @Overrideyouyour
	    public void reduce( final IntWritable pKey,
	                        final Iterable<DoubleWritable> pValues,
	                        final Context pContext )
	            throws IOException, InterruptedException{
	        int count = 0;
	        double sum = 0;
	        for ( final DoubleWritable value : pValues ){
	            sum += value.get();
	            count++;
	        }
	
	        final double avg = sum / count;
		
	        BasicBSONObject output = new BasicBSONObject();
	        output.put("avg", avg);
	        pContext.write( pKey, new BSONWritable( output ) );
	    }	
	}
	
###Pig

We can also easily accomplish the same task with just a few lines of Pig script. We also use some external UDFs provided by the Amazon Piggybank jar: http://aws.amazon.com/code/Elastic-MapReduce/2730

    -- UDFs used for date parsing
	REGISTER /tmp/piggybank-0.3-amzn.jar
	-- MongoDB Java driver
	REGISTER  /tmp/mongo-2.10.1.jar;
	-- Core Mongo-Hadoop Library
	REGISTER ../core/target/mongo-hadoop-core_1.0.3-1.1.0-SNAPSHOT.jar
	-- mongo-hadoop pig support
	REGISTER ../pig/target/mongo-hadoop-pig_1.0.3-1.1.0-SNAPSHOT.jar
	
	raw = LOAD 'mongodb://localhost:27017/demo.yield_historical.in' using com.mongodb.hadoop.pig.MongoLoader; 
	DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
	DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();
	
	date_tenyear = foreach raw generate UnixToISO($0#'_id'), $0#'bc10Year';
	parsed_year = foreach date_tenyear generate 
	    FLATTEN(EXTRACT($0, '(\\d{4})')) AS year, (double)$1 as bc;
	
	by_year = GROUP parsed_year BY (chararray)year;
	year_10yearavg = FOREACH by_year GENERATE group, AVG(parsed_year.bc) as tenyear_avg;
	
	-- Args to MongoInsertStorage are: schema for output doc, field to use as '_id'.
	STORE year_10yearavg 
	 INTO 'mongodb://localhost:27017/demo.asfkjabfa' 
	 USING		
	 com.mongodb.hadoop.pig.MongoInsertStorage('group:chararray,tenyear_avg:float', 'group');



## Example 2 - Enron E-mails

###Setup

Download a copy of the data set [here](http://mongodb-enron-email.s3-website-us-east-1.amazonaws.com/). Each document in the data set contains a single e-mail, including headers containing sender and recipient information. In this example we will build a list of the unique sender/recipient pairs, counting how many times each pair occurs.

Abbreviated code snippets shown below - to see the full source for this example, please see [here](http://github.com/mongodb/mongo-hadoop/examples/enron/src/) 

####Map/Reduce with Java

The mapper class will get the `headers` field from each document, parse out the sender from the `From` field and the recipients from the `To` field, and construct a `MailPair` object containing each pair which will act as the key. Then we emit the value `1` for each key. `MailPair` is just a simple "POJO" that contains Strings for the `from` and `to` values, and implements `WritableComparable` so that it can be serialized across Hadoop nodes and sorted. 	
	
	@Override
	public void map(NullWritable key, BSONObject val, final Context context)
        throws IOException, InterruptedException{
		if(val.containsKey("headers")){
			BSONObject headers = (BSONObject)val.get("headers");
			if(headers.containsKey("From") && headers.containsKey("To")){
				String from = (String)headers.get("From");
				String to = (String)headers.get("To");
                String[] recips = to.split(",");
                for(int i=0;i<recips.length;i++){
                    String recip = recips[i].trim();
                    if(recip.length() > 0){
                        context.write(new MailPair(from, recip), new IntWritable(1));
                    }
                }
			}
		}
	}


The reduce class will take the collected values for each key, sum them together, and record the output.

    @Override
    public void reduce( final MailPair pKey,
                        final Iterable<IntWritable> pValues,
                        final Context pContext )
            throws IOException, InterruptedException{
        int sum = 0;
        for ( final IntWritable value : pValues ){
            sum += value.get();
        }
        BSONObject outDoc = new BasicDBObjectBuilder().start().add( "f" , pKey.from).add( "t" , pKey.to ).get();
        BSONWritable pkeyOut = new BSONWritable(outDoc);
        pContext.write( pkeyOut, new IntWritable(sum) );
    }

####Pig

To accomplish the same with pig, but with much less work:

	REGISTER ../mongo-2.10.1.jar;
	REGISTER ../core/target/mongo-hadoop-core_cdh4.3.0-1.1.0.jar
	REGISTER ../pig/target/mongo-hadoop-pig_cdh4.3.0-1.1.0.jar
	
	raw = LOAD 'file:///Users/mike/dump/enron_mail/messages.bson' using com.mongodb.hadoop.pig.BSONLoader('','headers:[]') ; 
	send_recip = FOREACH raw GENERATE $0#'From' as from, $0#'To' as to;
	send_recip_filtered = FILTER send_recip BY to IS NOT NULL;
	send_recip_split = FOREACH send_recip_filtered GENERATE from as from, FLATTEN(TOKENIZE(to)) as to;
	send_recip_split_trimmed = FOREACH send_recip_split GENERATE from as from, TRIM(to) as to;
	send_recip_grouped = GROUP send_recip_split_trimmed BY (from, to);
	send_recip_counted = FOREACH send_recip_grouped GENERATE group, COUNT($1) as count;
	STORE send_recip_counted INTO 'file:///tmp/enron_emailcounts.bson' using com.mongodb.hadoop.pig.BSONStorage;

## Example 3 - Sensor Logs

### Problem

This example will deal with a basic example that does a "join" across two different collections, and will demonstrate using `MongoUpdateWritable` which lets you do complex updates when writing output records (instead of simple inserts).

Assume we have a collection called `devices`, each document contains the description of a sensor which records a particular type of data, for example:

	{
	  "_id": ObjectId("51b792d381c3e67b0a18d0ed"),
	  "name": "730LsNaN",
	  "type": "pressure",
	  "owner": "lswNxts07k",
	  "model": 18,
	  "created_at": ISODate("2003-12-02T11:15:09.555-0500")
	}

A second collection called `logs` contains data recorded by these sensors. Each document records the `_id` of the device it came from in the `d_id` field, the value, the timestamp when it was recorded, and the device's location at the time. The `logs` collection will be much larger than the `devices` collection, since each device will record potentially thousands of data points.

	{
	  "_id": ObjectId("51b792d381c3e67b0a18d678"),
	  "d_id": ObjectId("51b792d381c3e67b0a18d4a1"),
	  "v": 3328.5895416489802,
	  "timestamp": ISODate("2013-05-18T13:11:38.709-0400"),
	  "loc": [
	    -175.13,
	    51.658
	  ]
	}

As an example, let's solve an aggregation problem involving *both* of these collections - calculate the number of log entries for each owner, for each type of sensor (heat, pressure, etc).

We will solve this by doing two passes of Map/Reduce. The first will operate over the `devices` collection and seed an output collection by building a list of all the devices belonging to each owner. Then we will do a second pass over the `logs` collection, computing the totals by using `$inc` on the pre-seeded output collection.

####Phase One

The `Mapper` code in phase one just produces the pair **`<owner,_id`>** for each device. The `Reducer` then takes the list of all `_id`s for each owner and creates a new document containing them. 

	public class DeviceMapper extends Mapper<Object, BSONObject, Text, Text>{
	
		@Override
		public void map(Object key, BSONObject val, final Context context) 
		    throws IOException, InterruptedException {
	        String keyOut = (String)val.get("owner") + " " + (String)val.get("type");
	        context.write(new Text(keyOut), new Text(val.get("_id").toString()));
	    }
	
	}
	
	public class DeviceReducer extends Reducer<Text, Text, NullWritable, MongoUpdateWritable>{

	    @Override
	    public void reduce( final Text pKey, final Iterable<Text> pValues,
	                        final Context pContext )
	            throws IOException, InterruptedException{
	        
	        BasicBSONObject query = new BasicBSONObject("_id", pKey.toString());
	        ArrayList<ObjectId> devices = new ArrayList<ObjectId>();
	        for(Text val : pValues){
	            devices.add(new ObjectId(val.toString()));
	        }
			BasicBSONObject devices_list = new BasicBSONObject("devices", devices);
	        BasicBSONObject output = new BasicBSONObject("_id", pKey);
	        pContext.write(null, new BSONWritable(query, update, true, false));
	    }
	}



After phase one, the output collection documents each look like this:

	{
	  "_id": "1UoTcvnCTz temp",
	  "devices": [
	    ObjectId("51b792d381c3e67b0a18d475"),
	    ObjectId("51b792d381c3e67b0a18d16d"),
	    ObjectId("51b792d381c3e67b0a18d2bf"),
	    …
	  ]
	}
 
####Phase Two

In phase two, we map over the large collection `logs` and compute the totals for each device owner/type. The mapper emits the device id from each log item along, and the reducer uses `MongoUpdateWritable` to increment counts of these into the output collection by querying the record that contains the device's `_id` in its `devices` array which we populated in phase one. Between phase one and phase two, we create an index on `devices` to make sure this will be fast (see the script in `examples/sensors/run_job.sh` for details)

	public class LogMapper extends Mapper<Object, BSONObject, Text, IntWritable>{
		@Override
		public void map(Object key, BSONObject val, 
		                final Context context)
		                throws IOException, InterruptedException{
	        context.write(new Text(((ObjectId)val.get("d_id")).toString()), 
	                      new IntWritable(1));
	    }
	}
	
	public class LogReducer extends Reducer<Text, IntWritable, NullWritable, MongoUpdateWritable> {

	    @Override
	    public void reduce( final Text pKey,
	                        final Iterable<IntWritable> pValues,
	                        final Context pContext )
	            throws IOException, InterruptedException{
	        
	        int count = 0;
	        for(IntWritable val : pValues){
	            count += val.get();
	        }
	
	        BasicBSONObject query = new BasicBSONObject("devices", new ObjectId(pKey.toString()));
	        BasicBSONObject update = new BasicBSONObject("$inc", new BasicBSONObject("logs_count", count));
	        pContext.write(null, new MongoUpdateWritable(query, update, true, false));
	    }
	
	}

After phase two is finished, the result documents look like this (the `logs_count` field is now populated with the result):

	{
	  "_id": "1UoTcvnCTz temp",
	  "devices": [
	    ObjectId("51b792d381c3e67b0a18d475"),
	    ObjectId("51b792d381c3e67b0a18d16d"),
	    ObjectId("51b792d381c3e67b0a18d2bf"),
	    …
	  ],
	  "logs_count": 1050616
	}

