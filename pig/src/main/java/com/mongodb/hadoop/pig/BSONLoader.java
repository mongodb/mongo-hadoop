package com.mongodb.hadoop.pig;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
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
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.BSONFileInputFormat;

public class BSONLoader extends LoadFunc implements LoadMetadata {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static final Log LOG = LogFactory.getLog(BSONLoader.class);
    private final BSONFileInputFormat inputFormat = new BSONFileInputFormat();
    //CHECKSTYLE:OFF
    protected RecordReader in = null;

    private ResourceFieldSchema[] fields;
    protected ResourceSchema schema = null;
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
                // Schemaless mode - just output a tuple with a single element,
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
                    t.set(i, BSONLoader.readField(val.get(fieldTemp), fields[i]));
                }
            }
            return t;
        } catch (InterruptedException e) {
            throw new ExecException("Error while reading input", 6018);
        }

    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected static Object readField(final Object obj, final ResourceFieldSchema field) throws IOException {
        if (obj == null) {
            return null;
        }

        try {
            if (field == null) {
                //If we don't know the type we're using, try to convert it directly.
                return convertBSONtoPigType(obj);
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
                    return new DataByteArray(obj.toString());
                case DataType.CHARARRAY:
                    return obj.toString();
                case DataType.TUPLE:
                    ResourceSchema s = field.getSchema();
                    ResourceFieldSchema[] fs = s.getFields();
                    Tuple t = tupleFactory.newTuple(fs.length);

                    BasicBSONObject val = (BasicBSONObject) obj;

                    for (int j = 0; j < fs.length; j++) {
                        t.set(j, readField(val.get(fs[j].getName()), fs[j]));
                    }

                    return t;

                case DataType.BAG:
                    //We already know the bag has a schema of length 1 which is
                    //a tuple, so skip that schema and get the schema of the tuple.
                    s = field.getSchema();
                    ResourceFieldSchema[] bagFields = s.getFields();

                    s = bagFields[0].getSchema();

                    DataBag bag = bagFactory.newDefaultBag();
                    BasicBSONList vals = (BasicBSONList) obj;

                    if (s == null) {
                        //Handle lack of schema - We'll create a separate tuple for each item in this bag.
                        for(int j = 0; j < vals.size(); j++) {
                            t = tupleFactory.newTuple(1);
                            t.set(0, readField(vals.get(j), null));
                            bag.add(t);
                        }
                    } else {
                        fs = s.getFields();
                        for (Object val1 : vals) {
                            t = tupleFactory.newTuple(fs.length);

                            for(int k = 0; k < fs.length; k++) {
                                String fieldName = fs[k].getName();
                                t.set(k, readField(((BasicBSONObject) val1).get(fieldName), fs[k]));
                            }
                            bag.add(t);
                        }
                    }

                    return bag;

                case DataType.MAP:
                    s = field.getSchema();
                    fs = s != null ? s.getFields() : null;
                    BasicBSONObject inputMap = (BasicBSONObject) obj;

                    Map outputMap = new HashMap();
                    for (String key : inputMap.keySet()) {
                        if (fs != null) {
                            outputMap.put(key, readField(inputMap.get(key), fs[0]));
                        } else {
                            outputMap.put(key, convertBSONtoPigType(inputMap.get(key)));
                        }
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
            return null;
        }
    }

    public static Object convertBSONtoPigType(final Object o) throws ExecException {
        if (o == null) {
            return null;
        } else if (o instanceof Number || o instanceof String) {
            return o;
        } else if (o instanceof Date) {
            return ((Date) o).getTime();
        } else if (o instanceof ObjectId) {
            return o.toString();
        } else if (o instanceof BasicBSONList) {
            BasicBSONList bl = (BasicBSONList) o;
            DataBag bag = bagFactory.newDefaultBag();
            
            for(int i = 0; i < bl.size(); i++) {
                Tuple t = tupleFactory.newTuple(1);
                t.set(0, convertBSONtoPigType(bl.get(i)));
                bag.add(t);
            }

            return bag;
        } else if (o instanceof Map) {
            //TODO make this more efficient for lazy objects?
            Map<String, Object> fieldsMap = (Map<String, Object>) o;
            HashMap<String, Object> pigMap = new HashMap<String, Object>(fieldsMap.size());
            for (Map.Entry<String, Object> field : fieldsMap.entrySet()) {
                pigMap.put(field.getKey(), convertBSONtoPigType(field.getValue()));
            }
            return pigMap;
        } else {
            return o;
        }

    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        if (schema != null) {
            return schema;
        } else {
            try {
                //If we didn't have a schema, we loaded the document as a map.
                return new ResourceSchema(Utils.getSchemaFromString("document:map[]"));
            } catch (ParserException e) {
                //Should never get here, but just return null to indicate lack of a schema.
                return null;
            }
        }
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        // No statistics available. In the future
        // we could maybe construct something from db.collection.stats() here
        // but the class/API for this is unstable anyway, so this is unlikely
        // to be high priority.
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        // No partition keys. 
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
    }
}
