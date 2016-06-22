package com.mongodb.hadoop.pig;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.MongoClient;
import com.mongodb.hadoop.BSONFileInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.Utils;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.codecs.Encoder;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BSONLoader extends LoadFunc {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static final Log LOG = LogFactory.getLog(BSONLoader.class);
	private static final Encoder dbObjectCodec = new MyDBObjectCodec(MongoClient.getDefaultCodecRegistry());
    private final BSONFileInputFormat inputFormat = new BSONFileInputFormat();
    //CHECKSTYLE:OFF
    protected RecordReader in = null;

    private ResourceFieldSchema[] fields;
    protected ResourceSchema schema = null;
    private String[] inputFields;
    //CHECKSTYLE:ON
    private String idAlias = null;


    public BSONLoader() {
    }

    public BSONLoader(final String idAlias, final String userSchema) {
        this.idAlias = idAlias;
        try {
            schema = new ResourceSchema(Utils.getSchemaFromString(userSchema));
            fields = schema.getFields();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Schema Format");
        }
    }

    public BSONLoader(final String idAlias, final String userSchema, final String inputFields) {
        this(idAlias, userSchema);
    	this.inputFields = inputFields.split(",");
        if (fields != null && fields.length != this.inputFields.length)
        	throw new IllegalArgumentException("Input fields should have the same amount of fields as user schema");
    }

    @Override
    public void setLocation(final String location, final Job job) throws IOException {
        BSONFileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() {
        return this.inputFormat;
    }

    @Override
    public void prepareToRead(final RecordReader reader, final PigSplit split) {
        this.in = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        BSONObject val;
        try {
            if (!in.nextKeyValue()) {
                return null;
            }
            val = (BSONObject) in.getCurrentValue();

            Tuple t;
            if (this.fields == null) {
                // dynamic schema mode - just output a tuple with a single element,
                // which is a map storing the keys/vals in the document
                t = tupleFactory.newTuple(1);
                t.set(0, BSONLoader.convertBSONtoPigType(val));
            } else {
                t = tupleFactory.newTuple(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    String fieldTemp = fields[i].getName();
                    if (this.idAlias != null && this.idAlias.equals(fieldTemp)) {
                        fieldTemp = "_id";
                    }
                    if (inputFields != null)
                    	fieldTemp = inputFields[i];
                    t.set(i, BSONLoader.readField(val.get(fieldTemp), fields[i]));
                }
            }
            return t;
        } catch (InterruptedException e) {
            throw new ExecException("Error while reading input", 6018);
        }

    }

    /**
     * Convert an object from a MongoDB document into a type that Pig can
     * understand, based on the expectations of the given schema.
     * @param obj object from a MongoDB document
     * @param field the schema describing this field
     * @return an object appropriate for Pig
     * @throws IOException
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected static Object readField(final Object obj, final ResourceFieldSchema field) throws IOException {
        if (obj == null) {
            return null;
        }

        try {
            if (field == null) {
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
                    return BSONLoader.convertBSONtoPigType(obj);
                case DataType.CHARARRAY:
                    return toChararray(obj);
                case DataType.DATETIME:
                    return new DateTime(obj);
                case DataType.TUPLE:
                    ResourceSchema s = field.getSchema();
                    ResourceFieldSchema[] fs = s.getFields();
                    Tuple t = tupleFactory.newTuple(fs.length);

                    BasicDBObject val = (BasicDBObject) obj;

                    for (int j = 0; j < fs.length; j++) {
                        t.set(j, readField(val.get(fs[j].getName()), fs[j]));
                    }

                    return t;

                case DataType.BAG:
                    s = field.getSchema();
                    fs = s.getFields();

                    s = fs[0].getSchema();
                    fs = s.getFields();

                    DataBag bag = bagFactory.newDefaultBag();

                    BasicDBList vals = (BasicDBList) obj;

                    for (Object val1 : vals) {
                        t = tupleFactory.newTuple(fs.length);
                        for (int k = 0; k < fs.length; k++) {
                            t.set(k, readField(((BasicDBObject) val1).get(fs[k].getName()), fs[k]));
                        }
                        bag.add(t);
                    }

                    return bag;

                case DataType.MAP:
                    s = field.getSchema();
                    fs = s != null ? s.getFields() : null;
                    Map outputMap = new HashMap();
                    if (obj instanceof BSONObject) {
                        BasicBSONObject inputMap = (BasicBSONObject) obj;
                        for (String key : inputMap.keySet()) {
                            if (fs != null) {
                                outputMap.put(key,
                                  readField(inputMap.get(key), fs[0]));
                            } else {
                                outputMap.put(key,
                                  readField(inputMap.get(key), null));
                            }
                        }
                    } else if (obj instanceof DBRef) {
                        DBRef ref = (DBRef) obj;
                        outputMap.put("$ref", ref.getCollectionName());
                        outputMap.put("$id", ref.getId().toString());
                    }
                    return outputMap;
                default:
                    LOG.info("asfkjabskfjbsaf default for " + field.getName());
                    return BSONLoader.convertBSONtoPigType(obj);
            }
        } catch (Exception e) {
            String fieldName = field.getName() == null ? "" : field.getName();
            String type = DataType.genTypeToNameMap().get(field.getType());
            LOG.warn("Type " + type + " for field " + fieldName + " can not be applied to " + obj.getClass().toString());
            LOG.warn(e.toString());
            return null;
        }

    }

    private static final Pattern ARRAY_JSON_PATTERN = Pattern.compile("\\{\\s*\\\"key\\\"\\s*:\\s*(\\[.*\\])\\s*\\}");
    
	@SuppressWarnings("unchecked")
	protected static String toChararray(Object obj) {
		if (obj instanceof BasicDBObject) {
			return ((BasicDBObject) obj).toJson(dbObjectCodec);
		} else if (obj instanceof BasicDBList) {
            BasicDBList list = (BasicDBList) obj;
        	BasicDBObject bdo = new BasicDBObject("key", list);
        	String s = bdo.toJson(dbObjectCodec);
        	Matcher m = ARRAY_JSON_PATTERN.matcher(s);
        	if (m.matches())
        		return m.group(1);
        	LOG.warn("Could not parse object into array using regex: " + s);
		} else if (obj.equals(Float.NaN) || obj.equals(Double.NaN)) {
            return "\"NaN\"";
        } 
        return obj.toString();
	}


    /**
     * Convert an object from a MongoDB document into a type that Pig can
     * understand, based on the type of the input object.
     * @param o object from a MongoDB document
     * @return object appropriate for pig
     * @throws ExecException for lower-level Pig errors
     */
    public static Object convertBSONtoPigType(final Object o) throws ExecException {
        if (o == null) {
            return null;
        } else if (o instanceof Number || o instanceof String) {
            return o;
        } else if (o instanceof Date) {
            return ((Date) o).getTime();
        } else if (o instanceof ObjectId) {
            return o.toString();
        } else if (o instanceof UUID) {
            return o.toString();
        } else if (o instanceof BasicBSONList) {
            BasicBSONList bl = (BasicBSONList) o;
            Tuple t = tupleFactory.newTuple(bl.size());
            for (int i = 0; i < bl.size(); i++) {
                t.set(i, convertBSONtoPigType(bl.get(i)));
            }
            return t;
        } else if (o instanceof Map) {
            //TODO make this more efficient for lazy objects?
            Map<String, Object> fieldsMap = (Map<String, Object>) o;
            HashMap<String, Object> pigMap = new HashMap<String, Object>(fieldsMap.size());
            for (Map.Entry<String, Object> field : fieldsMap.entrySet()) {
                pigMap.put(field.getKey(), convertBSONtoPigType(field.getValue()));
            }
            return pigMap;
        } else if (o instanceof byte[]) {
            return new DataByteArray((byte[]) o);
        } else if (o instanceof Binary) {
            return new DataByteArray(((Binary) o).getData());
        } else if (o instanceof DBRef) {
            HashMap<String, String> pigMap = new HashMap<String, String>(2);
            pigMap.put("$ref", ((DBRef) o).getCollectionName());
            pigMap.put("$id", ((DBRef) o).getId().toString());
            return pigMap;
        } else {
            return o;
        }

    }

}
