
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
import com.mongodb.hadoop.MongoOutputFormat;
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
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

import java.io.IOException;
import java.util.Properties;

@SuppressWarnings("unchecked")
public class MongoInsertStorage extends StoreFunc implements StoreMetadata {

    private static final Log LOG = LogFactory.getLog(MongoStorage.class);
    // Pig specific settings
    static final String SCHEMA_SIGNATURE = "mongoinsert.pig.output.schema";
    //CHECKSTYLE:OFF
    protected ResourceSchema schema = null;
    //CHECKSTYLE:ON
    private RecordWriter out;

    private String udfcSignature = null;
    private String idField = null;

    private final MongoOutputFormat outputFormat = new MongoOutputFormat();

    public MongoInsertStorage() {
    }

    public MongoInsertStorage(final String idField) {
        this.idField = idField;
    }

    @Deprecated
    public MongoInsertStorage(final String idField, final String useUpsert) {
        this.idField = idField;
    }

    protected void writeField(final BasicDBObjectBuilder builder,
                              final ResourceSchema.ResourceFieldSchema field,
                              final Object d) throws IOException {
        Object convertedType = BSONStorage.getTypeForBSON(d, field, null);
        if (field.getName() != null && field.getName().equals(this.idField)) {
            builder.add("_id", convertedType);
        } else {
            if (field.getName() != null && field.getName().startsWith("underscore_")) {
                builder.add(field.getName().replace("underscore", ""), convertedType);
            } else {
            	builder.add(field.getName(), convertedType);
            }
        }

    }

    @Override
    public void checkSchema(final ResourceSchema schema) throws IOException {
        this.schema = schema;
        UDFContext udfc = UDFContext.getUDFContext();

        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty(SCHEMA_SIGNATURE, schema.toString());
    }

    @Override
    public void storeSchema(final ResourceSchema schema, final String location, final Job job) {
        // not implemented
    }

    @Override
    public void storeStatistics(final ResourceStatistics stats, final String location, final Job job) {
        // not implemented
    }

    @Override
    public void putNext(final Tuple tuple) throws IOException {
        try {
            final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
            ResourceFieldSchema[] fields = null;
            if (this.schema != null) {
                fields = this.schema.getFields();
            }
            if (fields != null) {
                for (int i = 0; i < fields.length; i++) {
                    writeField(builder, fields[i], tuple.get(i));
                }
            } else {
                for (int i = 0; i < tuple.size(); i++) {
                    writeField(builder, null, tuple.get(i));
                }
            }

            out.write(null, builder.get());
        } catch (Exception e) {
            throw new IOException("Couldn't convert tuple to bson: ", e);
        }
    }

    public void prepareToWrite(final RecordWriter writer) throws IOException {
        this.out = writer;
        if (this.out == null) {
            throw new IOException("Invalid Record Writer");
        }

        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        String strSchema = p.getProperty(SCHEMA_SIGNATURE);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        try {
            // Parse the schema from the string stored in the properties object.
            this.schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        } catch (Exception e) {
            this.schema = null;
            LOG.warn(e.getMessage());
        }

        LOG.info("GOT A SCHEMA " + this.schema + " " + strSchema);
    }

    public OutputFormat getOutputFormat() throws IOException {
        return this.outputFormat;
        //final MongoOutputFormat outputFmt = options == null ? new MongoOutputFormat() : new MongoOutputFormat(options.getUpdate().keys,
        // options.getUpdate().multi);
        //LOG.info( "OutputFormat... " + outputFmt );
        //return outputFmt;
    }

    public String relToAbsPathForStoreLocation(final String location, final Path curDir) throws IOException {
        // Don't convert anything - override to keep base from messing with URI
        return location;
    }

    public void setStoreLocation(final String location, final Job job) throws IOException {
        final Configuration config = job.getConfiguration();
        LOG.info("Store Location Config: " + config + " For URI: " + location);
        if (!location.startsWith("mongodb://")) {
            throw new IllegalArgumentException("Invalid URI Format.  URIs must begin with a mongodb:// protocol string.");
        }
        MongoConfigUtil.setOutputURI(config, location);
    }


    @Override
    public void setStoreFuncUDFContextSignature(final String signature) {
        udfcSignature = signature;
    }

}
