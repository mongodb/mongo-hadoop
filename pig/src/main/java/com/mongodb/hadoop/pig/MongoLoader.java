package com.mongodb.hadoop.pig;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;

public class MongoLoader extends LoadFunc implements LoadMetadata {
    private static final Log LOG = LogFactory.getLog(MongoStorage.class);
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    // Pig specific settings
    //CHECKSTYLE:OFF
    protected ResourceSchema schema = null;
    //CHECKSTYLE:ON
    private RecordReader in = null;
    private final MongoInputFormat inputFormat = new MongoInputFormat();
    private ResourceFieldSchema[] fields;
    private String idAlias = null;

    private final static CommandLineParser parser = new GnuParser();
    private final Options validOptions = new Options();
    boolean loadAsChararray = false;

    @Override
    public void setUDFContextSignature(final String signature) {
    }

    public MongoLoader() {
        LOG.info("Initializing MongoLoader in schemaless mode.");
        this.schema = null;
        this.fields = null;
    }


    
    public MongoLoader(final String userSchema) {
        this(userSchema, null);
    }

    public MongoLoader(final String userSchema, final String idAlias) {
        initializeSchema(userSchema, idAlias);
    }

    public MongoLoader(final String userSchema, final String idAlias, final String options) {
        String[] optsArr = options.split(" ");
        populateValidOptions();
        try {
            CommandLine configuredOptions = parser.parse(validOptions, optsArr);
            loadAsChararray = configuredOptions.hasOption("loadaschararray");
            initializeSchema(userSchema, idAlias);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid Options " + e.getMessage(), e);
        }
    }
    
    private void initializeSchema(final String userSchema, final String idAlias) {
        try {
            //Remove new lines from schema string.
            schema = new ResourceSchema(Utils.getSchemaFromString(userSchema.replaceAll("\\s|\\\\n","")));
            fields = schema.getFields();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Schema Format", e);
        }
    }
    
    private void populateValidOptions() {
        validOptions.addOption("loadaschararray", false, "Loads the entire record as a chararray");
    }

    public ResourceFieldSchema[] getFields() {
        return this.fields;
    }
    
    @Override
    public void setLocation(final String location, final Job job) throws IOException {
        MongoConfigUtil.setInputURI(job.getConfiguration(), location);

    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return this.inputFormat;
    }

    @Override
    public void prepareToRead(final RecordReader reader, final PigSplit split) throws IOException {
        this.in = reader;
        if (in == null) {
            throw new IOException("Invalid Record Reader");
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
        if (loadAsChararray) {
            if (fields != null && (fields.length != 1 || fields[0].getType() != DataType.CHARARRAY)) {
                throw new IllegalArgumentException("Invalid schema.  If -loadaschararray option is used, schema must be one chararray field.") ;
            }
            t = tupleFactory.newTuple(1);
            t.set(0, JSON.serialize(val));
            return t;
        }
        if (this.fields == null) {
            // Schemaless mode - just output a tuple with a single element,
            // which is a map storing the keys/values in the document
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
    }

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

}
