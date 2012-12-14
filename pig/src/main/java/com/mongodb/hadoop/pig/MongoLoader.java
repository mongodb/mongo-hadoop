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
//import org.apache.pig.parser.ParserException;
import org.bson.BSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.input.MongoRecordReader;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoLoader extends LoadFunc implements LoadMetadata {
	private static final Log log = LogFactory.getLog( MongoStorage.class );
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
    // Pig specific settings
    static final String PIG_INPUT_SCHEMA = "mongo.pig.input.schema";
    static final String PIG_INPUT_SCHEMA_UDF_CONTEXT = "mongo.pig.input.schema.udf_context";
    protected ResourceSchema schema = null;
    
    ResourceFieldSchema[] fields;
    
    public MongoLoader () {
    	throw new IllegalArgumentException("Undefined Schema");
    }
    
    public MongoLoader (String userSchema) {
    	try {
			schema = new ResourceSchema(Utils.getSchemaFromString(userSchema));
			fields = schema.getFields();
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid Schema Format");
		}
    }

	@Override
	public void setLocation(String location, Job job) throws IOException {
		final Configuration config = job.getConfiguration();
        log.info( "Load Location Config: " + config + " For URI: " + location );
        if ( !location.startsWith( "mongodb://" ) ) {
            throw new IllegalArgumentException("Invalid URI Format.  URIs must begin with a mongodb:// protocol string." );
        }
        MongoConfigUtil.setInputURI( config, location );
        
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		final MongoInputFormat inputFormat = new MongoInputFormat();
		log.info("InputFormat..." + inputFormat);
		return inputFormat;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		this._recordReader = (MongoRecordReader) reader;
		log.info("Preparing to read with " + _recordReader);
		
		if(_recordReader == null) {
			throw new IOException("Invalid Record Reader");
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
    protected Object readField(Object obj, ResourceFieldSchema field) throws IOException {
		if(obj == null)
			return null;
		
		try {
			if(field == null){
				return obj;
			}

    		switch (field.getType()) {
    		case DataType.INTEGER:
    			return Integer.parseInt(obj.toString());
    		case DataType.LONG:
    			return Long.parseLong(obj.toString());
    		case DataType.FLOAT:
    			return Float.parseFloat(obj.toString());
    		case DataType.DOUBLE:
    			return Double.parseDouble(obj.toString());
    		case DataType.BYTEARRAY:
    			return obj;
    		case DataType.CHARARRAY:
    			return obj.toString();
    		case DataType.TUPLE:
    			ResourceSchema s = field.getSchema();
    			ResourceFieldSchema[] fs = s.getFields();
    			Tuple t = tupleFactory.newTuple(fs.length);
    			
    			BasicDBObject val = (BasicDBObject)obj;
    			
    			for(int j = 0; j < fs.length; j++) {
    				t.set(j, readField(val.get(fs[j].getName()) ,fs[j]));
    			}
    			
    			return t;
    			
    		case DataType.BAG:
    			s = field.getSchema();
    			fs = s.getFields();
    			
    			s = fs[0].getSchema();
    			fs = s.getFields();
    			
    			DataBag bag = bagFactory.newDefaultBag();
    			
    			BasicDBList vals = (BasicDBList)obj;
    						
    			for(int j = 0; j < vals.size(); j++) {
    				t = tupleFactory.newTuple(fs.length);
    				for(int k = 0; k < fs.length; k++) {
    					t.set(k, readField(((BasicDBObject)vals.get(j)).get(fs[k].getName()), fs[k]));
    				}
    				bag.add(t);
    			}
    			
    			return bag;

    		case DataType.MAP:
                s = field.getSchema();
                fs = s != null ? s.getFields() : null;
                BasicDBObject inputMap = (BasicDBObject) obj;
                
                Map outputMap = new HashMap();
                for (String key : inputMap.keySet()) {
					if(fs != null){
						outputMap.put(key, readField(inputMap.get(key), fs[0]));
					}else{
						outputMap.put(key, readField(inputMap.get(key), null));
					}
                }
                return outputMap;

    		default:
    			return obj;
    		}
		} catch (Exception e) {
		    String fieldName = field.getName() == null ? "" : field.getName();
		    String type = DataType.genTypeToNameMap().get(field.getType());
		    log.warn("Type " + type + " for field " + fieldName + " can not be applied to " + obj.toString() + ".  Returning null.");
		    return null;
		}
		
	}
	
	@Override
	public Tuple getNext() throws IOException {
		BSONObject val = null;
		try {
			if(!_recordReader.nextKeyValue()) return null;
			
			val = _recordReader.getCurrentValue();
		}
		catch (Exception ie) {
			throw new IOException(ie);
		}
		
		Tuple t = tupleFactory.newTuple(fields.length);
		
		for(int i = 0; i < fields.length; i++) {
			t.set(i, readField(val.get(fields[i].getName()), fields[i]));
		}

		return t;
	}
	
	public String relativeToAbsolutePath(String location, org.apache.hadoop.fs.Path curDir)
     throws IOException {
        // Don't convert anything - override to keep base from messing with URI
        log.info( "Converting path: " + location + "(curDir: " + curDir + ")" );
        return location;
    }
    
    @Override
    public void setUDFContextSignature( String signature ){
        _udfContextSignature = signature;
    }

    String _udfContextSignature = null;
    MongoRecordReader _recordReader = null;

	@Override
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		if (schema != null) {
			return schema;
		}
		return null;
	}

	@Override
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
		// TODO Auto-generated method stub
		
	}
}
