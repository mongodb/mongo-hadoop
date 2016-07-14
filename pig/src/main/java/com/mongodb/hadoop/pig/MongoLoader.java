package com.mongodb.hadoop.pig;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class MongoLoader extends LoadFunc
  implements LoadMetadata, LoadPushDown {
    private static final Log LOG = LogFactory.getLog(MongoStorage.class);
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    // Pig specific settings
    private ResourceSchema schema = null;
    private RecordReader in = null;
    private final MongoInputFormat inputFormat = new MongoInputFormat();
    private ResourceFieldSchema[] fields = null;
    private HashMap<String, ResourceFieldSchema> schemaMapping;
    private List<String> projectedFields;
    private String idAlias = null;
    private String signature;
    private Map<String, String> inputFieldNames;
    
    private String query = null;
    
    private final static String PROJECTION_INPUT_FIELDS = MongoConfigUtil.INPUT_FIELDS + ".projection";


    public MongoLoader() {
        LOG.info("Initializing MongoLoader in dynamic schema mode.");
        schema = null;
        inputFieldNames = null;
    }

    public MongoLoader(final String userSchema) {
        this(userSchema, null);
    }

    public MongoLoader(final String userSchema, final String idAlias) {
        this.idAlias = idAlias;
        try {
            schema = new ResourceSchema(Utils.getSchemaFromString(userSchema));
            fields = schema.getFields();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Schema Format");
        }
    }
    
    public MongoLoader(final String userSchema, final String idAlias, final String inputFieldNamesStr) {
    	this(userSchema, idAlias);
        String[] inputFieldNamesArray = inputFieldNamesStr.split(",");
        if (fields != null && fields.length != inputFieldNamesArray.length)
        	throw new IllegalArgumentException("Input field names should have the same amount of fields as user schema");
        inputFieldNames = new HashMap<String, String>(fields.length);
        for (int i = 0; i < fields.length; i++) {
        	inputFieldNames.put(fields[i].getName(), inputFieldNamesArray[i]);
        }
    }

    public MongoLoader(final String userSchema, final String idAlias, final String inputFields, final String query) {
     	this(userSchema, idAlias, inputFields);
     	this.query = query;
     }

    @Override
    public void setUDFContextSignature(final String signature) {
        this.signature = signature;
    }

    private Properties getUDFProperties() {
        return UDFContext.getUDFContext()
          .getUDFProperties(getClass(), new String[]{signature});
    }

    public ResourceFieldSchema[] getFields() {
        return fields;
    }

    private BasicBSONObject getProjection() {
        String projectionStr =
          getUDFProperties().getProperty(PROJECTION_INPUT_FIELDS);
        return (BasicBSONObject) JSON.parse(projectionStr);
    }

    @Override
    public void setLocation(final String location, final Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        MongoConfigUtil.setInputURI(conf, location);
        String inputFieldsStr =
          getUDFProperties().getProperty(MongoConfigUtil.INPUT_FIELDS);
        if (inputFieldsStr != null) {
            conf.set(MongoConfigUtil.INPUT_FIELDS, inputFieldsStr);
        } else if (inputFieldNames != null){
        	conf.unset(MongoConfigUtil.INPUT_FIELDS);
        }
        try {
        	if (query != null)
        		MongoConfigUtil.setQuery( conf, query );
        	 else 
             	conf.unset(MongoConfigUtil.INPUT_QUERY);             
        } catch (Throwable e) {
        	throw new IllegalArgumentException("Could not set query.", e);
        }
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return inputFormat;
    }

    @Override
    public void prepareToRead(final RecordReader reader, final PigSplit split) throws IOException {
        in = reader;
        if (in == null) {
            throw new IOException("Invalid Record Reader");
        }

        BasicBSONObject projection = getProjection();
        if (fields != null && projection != null) {
            schemaMapping =
              new HashMap<String, ResourceFieldSchema>(fields.length);
            projectedFields = new ArrayList<String>();
            Set<String> visitedKeys = new HashSet<String>();
            // Prepare mapping of field name -> ResourceFieldSchema.
            for (ResourceFieldSchema fieldSchema : fields) {
                schemaMapping.put(fieldSchema.getName(), fieldSchema);
            }
            // Prepare list of projected fields.
            for (Map.Entry<String, Object> entry : projection.entrySet()) {
                boolean include = (Boolean) entry.getValue();
                // Add the name of the outer-level field if this is a nested
                // field. Pig will take care of pulling out the inner field.
                String key = StringUtils.split(entry.getKey(), '\\', '.')[0];
                if (include && !visitedKeys.contains(key)) {
                    projectedFields.add(key);
                    visitedKeys.add(key);
                }
            }
        }
    }

    @Override
    public Tuple getNext() throws IOException {
        BSONObject val;
        try {
            if (!in.nextKeyValue()) {
                return null;
            }
            val = (BSONObject) in.getCurrentValue();
        } catch (Exception ie) {
            throw new IOException(ie);
        }
        
        Tuple t;
        if (fields == null) {
            // dynamic schema mode - just output a tuple with a single element,
            // which is a map storing the keys/values in the document
            // Since there is no schema, no projection can be made, and
            // there's no need to worry about retrieving projected fields.
            t = tupleFactory.newTuple(1);
            t.set(0, BSONLoader.convertBSONtoPigType(val));
        } else {
            // A schema was provided. Try to retrieve the projection.
            int tupleSize;
            if (projectedFields != null) {
                tupleSize = projectedFields.size();
            } else {
                tupleSize = fields.length;
            }

            t = tupleFactory.newTuple(tupleSize);
            for (int i = 0; i < t.size(); i++) {
                String fieldTemp;
                ResourceFieldSchema fieldSchema;
                if (null == projectedFields) {
                    fieldTemp = fields[i].getName();
                    fieldSchema = fields[i];
                    if (inputFieldNames == null && idAlias != null && idAlias.equals(fieldTemp)) {
                        fieldTemp = "_id";
                    }
                } else {
                    fieldTemp = projectedFields.get(i);
                    // Use id alias in order to retrieve type info.
                    if (idAlias != null && "_id".equals(fieldTemp)) {
                        fieldSchema = schemaMapping.get(idAlias);
                    } else {
                        fieldSchema = schemaMapping.get(fieldTemp);
                    }
                }
                if (inputFieldNames != null && inputFieldNames.containsKey(fieldTemp))
                	fieldTemp = inputFieldNames.get(fieldTemp);
                t.set(i, BSONLoader.readField(val.get(fieldTemp), fieldSchema));
            }
        }
        return t;
    }

    @Override
    public String relativeToAbsolutePath(final String location, final Path curDir) throws IOException {
        // This is a mongo URI and has no notion of relative/absolute,
        // thus we want to always use just the same location.
        return location;
    }


    @Override
    public ResourceSchema getSchema(final String location, final Job job) throws IOException {
        if (schema != null) {
            return schema;
        }
        return null;
    }

    @Override
    public ResourceStatistics getStatistics(final String location, final Job job) throws IOException {
        // No statistics available. In the future
        // we could maybe construct something from db.collection.stats() here
        // but the class/API for this is unstable anyway, so this is unlikely
        // to be high priority.
        return null;
    }

    @Override
    public String[] getPartitionKeys(final String location, final Job job) throws IOException {
        // No partition keys.
        return null;
    }

    @Override
    public void setPartitionFilter(final Expression partitionFilter) throws IOException {
    }

    @Override
    public List<OperatorSet> getFeatures() {
        // PROJECTION is all that exists in the OperatorSet enum.
        return Collections.singletonList(OperatorSet.PROJECTION);
    }

    @Override
    public RequiredFieldResponse pushProjection(
      final RequiredFieldList requiredFieldList)
      throws FrontendException {
        // Cannot project any fields if there is no schema.
        // Currently, Pig won't even attempt projection without a schema
        // anyway, but more work will be needed if a future version supports
        // this.
        if (null == schema) {
            return new RequiredFieldResponse(false);
        }

        BSONObject projection = new BasicBSONObject();
        BSONObject projectionForQuery = new BasicBSONObject();
        boolean needId = false;
        for (RequiredField field : requiredFieldList.getFields()) {
            String fieldName = field.getAlias();
            if (inputFieldNames != null && inputFieldNames.containsKey(fieldName)) {
            	projectionForQuery.put(inputFieldNames.get(fieldName), true);            	
            } else if (idAlias != null && idAlias.equals(fieldName)) {
                fieldName = "_id";
                needId = true;
            }
            List<RequiredField> subFields = field.getSubFields();
            if (subFields != null && !subFields.isEmpty()) {
                // Pig is limited to populating at most one subfield level deep.
                for (RequiredField subField : subFields) {
                    projection.put(fieldName + "." + subField.getAlias(), true);
                }
            } else {
                projection.put(fieldName, true);
            }
        }
        // Turn off _id unless asked for.
        if (inputFieldNames == null && !needId) {
            projection.put("_id", false);
        }

        LOG.info("projection: " + projection);
        LOG.info("projectionForQuery: " + projectionForQuery);

        
        
        // Store projection to be retrieved later and stored into the job
        // configuration.
        getUDFProperties().setProperty(
                MongoConfigUtil.INPUT_FIELDS, JSON.serialize(projectionForQuery));
        getUDFProperties().setProperty(PROJECTION_INPUT_FIELDS, JSON.serialize(projection));

        // Return a response indicating that we can honor the projection.
        return new RequiredFieldResponse(true);
    }
}
