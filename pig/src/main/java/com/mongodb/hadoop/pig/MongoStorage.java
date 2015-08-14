/*
 * Copyright 2011 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.pig;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.output.MongoRecordWriter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MongoStorage extends StoreFunc implements StoreMetadata {

    private static final Log LOG = LogFactory.getLog(MongoStorage.class);
    // Pig specific settings
    static final String PIG_OUTPUT_SCHEMA = "mongo.pig.output.schema";
    static final String PIG_OUTPUT_SCHEMA_UDF_CONTEXT = "mongo.pig.output.schema.udf_context";
    //CHECKSTYLE:OFF
    protected ResourceSchema schema = null;
    //CHECKSTYLE:ON
    private final MongoStorageOptions options;

    private String udfContextSignature = null;
    private MongoRecordWriter recordWriter = null;

    public MongoStorage() {
        this.options = null;
    }

    /**
     * <p>
     * Takes a list of arguments of two types:
     * </p>
     * <ul>
     * <li>A single set of keys to base updating on in the format:
     * <code>'update [time, user]'</code> or <code>'multi [time, user]'</code> for multi updates</li>
     * <li>Multiple indexes to ensure in the format:
     * <code>'{time: 1, user: 1},{unique: true}'</code>
     * (The syntax is exactly like db.col.ensureIndex())</li>
     * </ul>
     * <p>
     * Example:
     * </p>
     * <pre><code>
     * STORE Result INTO '$db'
     * USING com.mongodb.hadoop.pig.MongoStorage(
     *   'update [time, * servername, hostname]',
     *   '{time : 1, servername : 1, hostname : 1}, {unique:true, dropDups: true}'
     * )
     * </code></pre>
     *
     * @param args storage arguments
     * @throws ParseException if the arguments cannot be parsed
     */
    public MongoStorage(final String... args) throws ParseException {
        this.options = MongoStorageOptions.parseArguments(args);
    }


    public void checkSchema(final ResourceSchema schema) throws IOException {
        LOG.info("checking schema " + schema.toString());
        this.schema = schema;
        final Properties properties =
            UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{udfContextSignature});
        properties.setProperty(PIG_OUTPUT_SCHEMA_UDF_CONTEXT, schema.toString());
    }

    public void storeSchema(final ResourceSchema schema, final String location, final Job job) {
        // not implemented
    }


    public void storeStatistics(final ResourceStatistics stats, final String location, final Job job) {
        // not implemented
    }


    public void putNext(final Tuple tuple) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("writing " + tuple.toString());
        }
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

        ResourceFieldSchema[] fields = this.schema.getFields();
        for (int i = 0; i < fields.length; i++) {
            writeField(builder, fields[i], tuple.get(i));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("writing out:" + builder.get().toString());
        }
        //noinspection unchecked
        recordWriter.write(null, builder.get());
    }

    protected void writeField(final BasicDBObjectBuilder builder,
                              final ResourceSchema.ResourceFieldSchema field,
                              final Object d) throws IOException {

        // If the field is missing or the value is null, write a null
        if (d == null) {
            builder.add(field.getName(), null);
            return;
        }

        ResourceSchema s = field.getSchema();

        // Based on the field's type, write it out
        byte i = field.getType();
        if (i == DataType.INTEGER) {
            builder.add(field.getName(), d);
        } else if (i == DataType.LONG) {
            builder.add(field.getName(), d);
        } else if (i == DataType.FLOAT) {
            builder.add(field.getName(), d);
        } else if (i == DataType.DOUBLE) {
            builder.add(field.getName(), d);
        } else if (i == DataType.BYTEARRAY) {
            builder.add(field.getName(), d.toString());
        } else if (i == DataType.CHARARRAY) {
            builder.add(field.getName(), d);
        } else if (i == DataType.TUPLE) {
            // Given a TUPLE, create a Map so BSONEncoder will eat it
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use this storage function.  No schema found for field "
                                      + field.getName());
            }
            ResourceFieldSchema[] fs = s.getFields();
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            for (int j = 0; j < fs.length; j++) {
                m.put(fs[j].getName(), ((Tuple) d).get(j));
            }
            builder.add(field.getName(), (Map) m);
        } else if (i == DataType.BAG) {
            // Given a BAG, create an Array so BSONEncoder will eat it.
            ResourceFieldSchema[] fs;
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use this storage function.  No schema found for field "
                                      + field.getName());
            }
            fs = s.getFields();
            if (fs.length != 1 || fs[0].getType() != DataType.TUPLE) {
                throw new IOException("Found a bag without a tuple "
                                      + "inside!");
            }
            // Drill down the next level to the tuple's schema.
            s = fs[0].getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use this storage function.  No schema found for field "
                                      + field.getName());
            }
            fs = s.getFields();

            List<Map<String, Object>> a = new ArrayList<Map<String, Object>>();
            for (Tuple t : (DataBag) d) {
                Map<String, Object> ma = new LinkedHashMap<String, Object>();
                for (int j = 0; j < fs.length; j++) {
                    ma.put(fs[j].getName(), t.get(j));
                }
                a.add(ma);
            }

            builder.add(field.getName(), a);
        } else if (i == DataType.MAP) {
            Map map = (Map) d;
            for (Object key : map.keySet()) {
                builder.add(key.toString(), map.get(key));
            }
        }
    }

    public void prepareToWrite(final RecordWriter writer) throws IOException {

        recordWriter = (MongoRecordWriter) writer;
        LOG.info("Preparing to write to " + recordWriter);
        if (recordWriter == null) {
            throw new IOException("Invalid Record Writer");
        }
        // Parse the schema from the string stored in the properties object.

        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{udfContextSignature});

        String strSchema = p.getProperty(PIG_OUTPUT_SCHEMA_UDF_CONTEXT);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        try {
            // Parse the schema from the string stored in the properties object.
            this.schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        if (options != null) {
            // If we are insuring any indexes do so now:
            for (MongoStorageOptions.Index in : options.getIndexes()) {
                recordWriter.ensureIndex(in.index, in.options);
            }
        }
    }

    public OutputFormat getOutputFormat() throws IOException {
        return new MongoOutputFormat();
    }

    public String relToAbsPathForStoreLocation(final String location, final Path curDir) throws IOException {
        // Don't convert anything - override to keep base from messing with URI
        return location;
    }

    public void setStoreLocation(final String location, final Job job) throws IOException {
        final Configuration config = job.getConfiguration();
        if (!location.startsWith("mongodb://")) {
            throw new IllegalArgumentException("Invalid URI Format.  URIs must begin with a mongodb:// protocol string.");
        }
        MongoClientURI locURI = new MongoClientURI(location);
        LOG.info(String.format(
            "Store location config: %s; for namespace: %s.%s; hosts: %s",
            config, locURI.getDatabase(), locURI.getCollection(),
            locURI.getHosts()));
        MongoConfigUtil.setOutputURI(config, locURI);
        final Properties properties =
            UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{udfContextSignature});
        config.set(PIG_OUTPUT_SCHEMA, properties.getProperty(PIG_OUTPUT_SCHEMA_UDF_CONTEXT));
    }

    public void setStoreFuncUDFContextSignature(final String signature) {
        udfContextSignature = signature;
    }
}
