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
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.bson.types.ObjectId;
import org.bson.BsonDateTime;
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
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Map;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Properties;

@SuppressWarnings("unchecked")
public class MongoInsertStorage extends StoreFunc implements StoreMetadata {

    // Pig specific settings
    static final String SCHEMA_SIGNATURE = "mongoinsert.pig.output.schema";
    private static final Log LOG = LogFactory.getLog(MongoStorage.class);
    private final MongoOutputFormat outputFormat = new MongoOutputFormat();
    //CHECKSTYLE:OFF
    protected ResourceSchema schema = null;
    //CHECKSTYLE:ON
    private RecordWriter out;
    private String udfcSignature = null;
    private String idField = null;
    private boolean wrapIdWithObjectId = false;
    private ArrayList<String> dateFields = new ArrayList<String>();
    private ArrayList<String> objectIdFields = new ArrayList<String>();

    public MongoInsertStorage() {
    }

    /**
     * @param idField   the field standing in for {@code _id}
     * @param useUpsert is parameter is unused
     * @deprecated useUpsert is unused. Use {@link #MongoInsertStorage(String)}
     * instead.
     */
    @Deprecated
    @SuppressWarnings("UnusedParameters")
    public MongoInsertStorage(final String idField, final String useUpsert) {
        this.idField = idField;
    }

    /**
     * Create a new MongoInsertStorage.
     *
     * @param idField the field standing in for {@code _id}
     */
    public MongoInsertStorage(final String idField) {
        this.idField = idField;
    }

    public MongoInsertStorage(
        final String idField, 
        final boolean wrapIdWithObjectId, 
        final String dateFields, 
        final String objectIdFields) {
        this.idField = idField;
        this.wrapIdWithObjectId = wrapIdWithObjectId;
        Collections.addAll(this.dateFields, dateFields.split(","));
        Collections.addAll(this.objectIdFields, objectIdFields.split(","));
    }

    public String relToAbsPathForStoreLocation(final String location, final Path curDir) throws IOException {
        // Don't convert anything - override to keep base from messing with URI
        return location;
    }

    public OutputFormat getOutputFormat() throws IOException {
        return outputFormat;
        //final MongoOutputFormat outputFmt = options == null ? new MongoOutputFormat() : new MongoOutputFormat(options.getUpdate().keys,
        // options.getUpdate().multi);
        //LOG.info( "OutputFormat... " + outputFmt );
        //return outputFmt;
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
    }

    @Override
    public void checkSchema(final ResourceSchema schema) throws IOException {
        this.schema = schema;
        UDFContext udfc = UDFContext.getUDFContext();

        Properties p = udfc.getUDFProperties(getClass(), new String[]{udfcSignature});
        p.setProperty(SCHEMA_SIGNATURE, schema.toString());
    }

    public void prepareToWrite(final RecordWriter writer) throws IOException {
        out = writer;
        if (out == null) {
            throw new IOException("Invalid Record Writer");
        }

        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(getClass(), new String[]{udfcSignature});
        String strSchema = p.getProperty(SCHEMA_SIGNATURE);
        if (strSchema == null) {
            LOG.warn("Could not find schema in UDF context. Interpreting each tuple as containing a single map.");
        } else {
            try {
                // Parse the schema from the string stored in the properties object.
                schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
            } catch (Exception e) {
                schema = null;
                LOG.warn(e.getMessage());
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("GOT A SCHEMA " + schema + " " + strSchema);
            }
        }
    }

    @Override
    public void putNext(final Tuple tuple) throws IOException {
        try {
            final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
            ResourceFieldSchema[] fields = null;
            if (schema != null) {
                fields = schema.getFields();
            }
            if (fields != null) {
                for (int i = 0; i < fields.length; i++) {
                    System.out.println("!!! The tuple and schema exists");
                    writeField(builder, fields[i], tuple.get(i));
                }
            } else {
                // Assume that the tuple contains only a map, as produced by
                // MongoLoader, for example.
                if (tuple.size() != 1) {
                    throw new IOException("Could not retrieve schema, but tuples did not contain a single item: " + tuple);
                }
                System.out.println("!!! The tuple has size 1");
                Object result = BSONStorage.getTypeForBSON(
                    tuple.get(0), null, null);
                if (!(result instanceof Map)) {
                    throw new IOException("Could not retrieve schema, but tuples contained something other than a Map: " + tuple);
                }
                Map<String, Object> documentMap = (Map<String, Object>) result;
                for (Map.Entry<String, Object> entry : documentMap.entrySet()) {
                    builder.add(entry.getKey(), entry.getValue());
                }
            }

            out.write(null, builder.get());
        } catch (Exception e) {
            throw new IOException("Couldn't convert tuple to bson: ", e);
        }
    }

    @Override
    public void setStoreFuncUDFContextSignature(final String signature) {
        udfcSignature = signature;
    }

    @Override
    public void storeStatistics(final ResourceStatistics stats, final String location, final Job job) {
        // not implemented
    }

    @Override
    public void storeSchema(final ResourceSchema schema, final String location, final Job job) {
        // not implemented
    }

    protected void writeField(final BasicDBObjectBuilder builder,
                              final ResourceFieldSchema field,
                              final Object d) throws IOException {
        Object convertedType = BSONStorage.getTypeForBSON(d, field, null);

        if (((String) d) == "drop field") {
            return;
        }

        if (field.getName() != null && field.getName().equals(idField)) {
            if (this.wrapIdWithObjectId) {
                builder.add("_id", new ObjectId(convertedType.toString()));
            } else {
                builder.add("_id", convertedType);
            }
        } else if (this.objectIdFields.contains(field.getName())) {
            builder.add(field.getName(), new ObjectId(convertedType.toString()));
        } else if (this.dateFields.contains(field.getName())) {
            builder.add(field.getName(), new BsonDateTime(((DateTime) d).getMillis()));
        } else {
            builder.add(field.getName(), convertedType);
        }
    }

}
