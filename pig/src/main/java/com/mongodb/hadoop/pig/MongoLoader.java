package com.mongodb.hadoop.pig;

import com.mongodb.*;
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.input.*;
import com.mongodb.hadoop.output.MongoRecordWriter;
import com.mongodb.hadoop.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.*;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldResponse;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.bson.BSONObject;


import java.io.*;
import java.text.ParseException;
import java.util.*;

public class MongoLoader extends LoadFunc implements LoadPushDown {
	private static final Log log = LogFactory.getLog( MongoStorage.class );
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
    // Pig specific settings
    static final String PIG_INPUT_SCHEMA = "mongo.pig.input.schema";
    static final String PIG_INPUT_SCHEMA_UDF_CONTEXT = "mongo.pig.input.schema.udf_context";
    protected ResourceSchema schema = null;
    
    ResourceFieldSchema[] fields;
    //private final MongoStorageOptions options;
    
    public MongoLoader () {
    
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
		
		if(_recordReader == null)
			throw new IOException("Invalid Record Reader");
		
		UDFContext udfc = UDFContext.getUDFContext();
		Configuration c = udfc.getJobConf();
		Properties p = udfc.getUDFProperties(this.getClass(), new String[]{_udfContextSignature});
		
		String strSchema = c.get("mongo.pig.output.schema");
		if(strSchema == null) {
			throw new IOException("Please define a schema");
		}
		
		try {
			schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
		}
		catch (Exception e) {
			e.printStackTrace();
        }

		fields = schema.getFields();
	}
	private Object readField(Object obj, ResourceFieldSchema field) throws IOException {
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
			return obj.toString().toCharArray();
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
		default:
			return obj;
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
		
		//log.info("Line:" + val.toString());
		
		//log.info("Projection String: " + this.projectionArray);
		/*ArrayList<Object> protoTuple = new ArrayList<Object>();
		
		for(String key: val.keySet())
		{
			if(projectionArray.contains(key)) {
				protoTuple.add(projectionArray.indexOf(key), val.get(key));
			}
		}
		
		Tuple t = tupleFactory.newTuple(protoTuple);*/
		
		//Tuple t = tupleFactory.newTuple(2);
		//t.set(0, "test");
		
		
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
	public List<OperatorSet> getFeatures() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RequiredFieldResponse pushProjection(
			RequiredFieldList requiredFieldList) throws FrontendException {
		log.info("HERE");
		return null;
	}


}
