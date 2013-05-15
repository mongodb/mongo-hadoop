package com.mongodb.hadoop.pig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.bson.BSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.input.MongoRecordReader;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoLoader extends LoadFunc implements LoadMetadata {
	private static final Log log = LogFactory.getLog( MongoStorage.class );
	private static TupleFactory tupleFactory = TupleFactory.getInstance();
	private static BagFactory bagFactory = BagFactory.getInstance();
    // Pig specific settings
    static final String PIG_INPUT_SCHEMA = "mongo.pig.input.schema";
    protected ResourceSchema schema = null;
    private RecordReader in = null;
    private String _udfContextSignature = null;
    private final MongoInputFormat inputFormat = new MongoInputFormat();
    private static final String PIG_INPUT_SCHEMA_UDF_CONTEXT = "mongo.pig.input.schema.udf_context";
    private ResourceFieldSchema[] fields;
    private String idAlias = null;

    @Override
    public void setUDFContextSignature( String signature ){
        _udfContextSignature = signature;
    }
    
    public MongoLoader () {
        log.info("Initializing MongoLoader in schemaless mode.");
        this.schema = null;
        this.fields = null;
    }

    public ResourceFieldSchema[] getFields(){
        return this.fields;
    }
    
    public MongoLoader(String userSchema, String idAlias) {
        this.idAlias = idAlias;
    	try {
			schema = new ResourceSchema(Utils.getSchemaFromString(userSchema));
			fields = schema.getFields();
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid Schema Format");
		}
    }

    public MongoLoader(String userSchema) {
        this(userSchema, null);
    }

	@Override
	public void setLocation(String location, Job job) throws IOException {
		final Configuration config = job.getConfiguration();
        if ( !location.startsWith( "mongodb://" ) ) {
            throw new IllegalArgumentException("Invalid URI Format.  URIs must begin with a mongodb:// protocol string." );
        }
        MongoConfigUtil.setInputURI( config, location );
        
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
        return this.inputFormat;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
		this.in = reader;
		if(in == null) {
			throw new IOException("Invalid Record Reader");
		}
	}
	
	@Override
	public Tuple getNext() throws IOException {
		BSONObject val = null;
		try {
			if(!in.nextKeyValue()) return null;
			val = (BSONObject)in.getCurrentValue();
		} catch (Exception ie) {
			throw new IOException(ie);
		}
		
        Tuple t;
        if( this.fields == null ){
            // Schemaless mode - just output a tuple with a single element,
            // which is a map storing the keys/vals in the document
            t = tupleFactory.newTuple(1);
            t.set(0, BSONLoader.convertBSONtoPigType(val));
        }else{
            t = tupleFactory.newTuple(fields.length);
            for(int i = 0; i < fields.length; i++) {
                String fieldTemp = fields[i].getName();
                if(this.idAlias != null && this.idAlias.equals(fieldTemp)){
                    fieldTemp = "_id";
                }
                t.set(i, BSONLoader.readField(val.get(fieldTemp), fields[i]));
            }
        }
        return t;
	}
	
	public String relativeToAbsolutePath(String location, org.apache.hadoop.fs.Path curDir) throws IOException {
        // This is a mongo URI and has no notion of relative/absolute,
        // thus we want to always use just the same location.
        return location;
    }
    

	@Override
	public ResourceSchema getSchema(String location, Job job) throws IOException {
		if (schema != null) {
			return schema;
		}
		return null;
	}

	@Override
	public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        // No statistics available. In the future
        // we could maybe construct something from db.collection.stats() here
        // but the class/API for this is unstable anyway, so this is unlikely
        // to be high priority.
		return null;
	}

	@Override
	public String[] getPartitionKeys(String location, Job job) throws IOException {
        // No partition keys. 
		return null;
	}

	@Override
	public void setPartitionFilter(Expression partitionFilter) throws IOException { }

}
