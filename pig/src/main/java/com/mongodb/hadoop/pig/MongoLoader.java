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
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
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
        try {
            //If no schema is passed in we'll load the whole document as a map.
            schema = new ResourceSchema(Utils.getSchemaFromString("document:map[]"));
        } catch (ParserException e) {
            //Should never get here
            throw new IllegalArgumentException("Error constructing default MongoLoader schema");
        }
    }
    
    public MongoLoader (String userSchema) {
        try {
            schema = new ResourceSchema(Utils.getSchemaFromString(userSchema));
            fields = schema.getFields();
        } catch (ParserException e) {
            throw new IllegalArgumentException("Invalid Schema Format: " + e.getMessage());
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
        return inputFormat;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        this._recordReader = (MongoRecordReader) reader;

        if(_recordReader == null) {
            throw new IOException("Invalid Record Reader");
        }
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Object readField(Object obj, ResourceFieldSchema field) throws IOException {
        if(obj == null)
            return null;

        if (field == null) {
            field = inferFieldSchema(obj);
        }

        try {
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
                return new DataByteArray(obj.toString());
            case DataType.CHARARRAY:
                return obj.toString();
            case DataType.TUPLE:
                ResourceSchema s = field.getSchema();
                ResourceFieldSchema[] fs = s.getFields();
                Tuple t = tupleFactory.newTuple(fs.length);

                if (obj instanceof BasicDBObject) {
                    BasicDBObject val = (BasicDBObject)obj;
                    for(int j = 0; j < fs.length; j++) {
                        t.set(j, readField(val.get(fs[j].getName()) ,fs[j]));
                    }
                } else if (obj instanceof BasicDBList) {
                    //This happens when a user wants to turn a list into a tuple.
                    BasicDBList vals = (BasicDBList) obj;
                    for (int j = 0; j < fs.length; j++) {
                        t.set(j, readField(vals.get(j), fs[j]));
                    }
                }
                return t;

            case DataType.BAG:
                //We already know the bag has tuples, so skip that schema and get the schema of the tuple.
                s = field.getSchema();
                ResourceFieldSchema[] tuplefs = s.getFields();
                s = tuplefs[0].getSchema();

                DataBag bag = bagFactory.newDefaultBag();
                BasicDBList vals = (BasicDBList)obj;

                if (s == null) {
                    //Handle lack of schema - We'll create a separate tuple for each item in this bag.
                    for(int j = 0; j < vals.size(); j++) {
                        t = tupleFactory.newTuple(1);
                        t.set(0, readField(vals.get(j), null));
                        bag.add(t);
                    }
                } else {
                    fs = s.getFields();
                    for(int j = 0; j < vals.size(); j++) {
                        t = tupleFactory.newTuple(fs.length);
                        Object embeddedField = vals.get(j);

                        if (embeddedField instanceof BasicDBObject) {
                            for(int k = 0; k < fs.length; k++) {
                                String fieldName = fs[k].getName();
                                t.set(k, readField(((BasicDBObject) embeddedField).get(fieldName), fs[k]));
                            }
                        } else {
                            //This happens when a user declares an array as a tuple in the schema.  
                            //We try to make that work to provide a way of retrieving an ordered list.
                            t = (Tuple) readField(embeddedField, tuplefs[0]);
                        }
                        bag.add(t);
                    }
                }

                return bag;

            case DataType.MAP:
                s = field.getSchema();
                
                //If no internal schema for the map was specified - use null and let readField infer the schema
                //by looking at the object
                ResourceFieldSchema innerFieldSchema = null;
                if (s != null) {
                    fs = s.getFields();
                    innerFieldSchema = fs[0];
                }

                BasicDBObject inputMap = (BasicDBObject) obj;
                
                Map outputMap = new HashMap();
                for (String key : inputMap.keySet()) {
                    outputMap.put(key, readField(inputMap.get(key), innerFieldSchema));
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
    
    protected ResourceFieldSchema inferFieldSchema(Object o) throws ParserException {
        if (o instanceof BasicDBList) {
            return getResourceFieldSchemaFromString("b:bag{t:tuple()}");
        } else if (o instanceof BasicDBObject) {
            return getResourceFieldSchemaFromString("m:map[]");
        } else if (o instanceof Integer) {
            return getResourceFieldSchemaFromString("i:int");
        } else if (o instanceof Long) {
            return getResourceFieldSchemaFromString("l:long");
        } else if (o instanceof Float) {
            return getResourceFieldSchemaFromString("f:float");
        } else if (o instanceof Double) {
            return getResourceFieldSchemaFromString("d:double");
        } else {
            return getResourceFieldSchemaFromString("c:chararray");
        }
    }

    private ResourceFieldSchema getResourceFieldSchemaFromString(String schemaString) throws ParserException {
        try {
            return new ResourceSchema(Utils.getSchemaFromString(schemaString)).getFields()[0];
        } catch (ParserException e) {
            log.error("Parser exception: ", e);
            throw e;
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
        
        Tuple t;
        if (fields != null) {
            t = tupleFactory.newTuple(fields.length);
            
            for(int i = 0; i < fields.length; i++) {
                t.set(i, readField(val.get(fields[i].getName()), fields[i]));
            }
        } else {
            t = tupleFactory.newTuple(1);
            t.set(0, readField(val, schema.getFields()[0]));
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
