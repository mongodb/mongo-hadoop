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

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.MongoUpdateWritable;
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
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.Properties;

/*
 * MongoUpdateStorage : used to update documents in a collection
 */
public class MongoUpdateStorage extends StoreFunc implements StoreMetadata {

    private static final Log LOG = LogFactory.getLog(MongoUpdateStorage.class);

    // Pig specific settings
    static final String SCHEMA_SIGNATURE = "mongoupdate.pig.output.schema";
    private ResourceSchema schema = null;
    private String signature = null;

    // private final MongoStorageOptions options;
    private final MongoOutputFormat outputFormat = new MongoOutputFormat();

    // MongoRecordWriter to use for updating MongoDB documents
    private MongoRecordWriter<?, MongoUpdateWritable> recordWriter = null;

    // JSONPigReplace setup
    private JSONPigReplace pigReplace;
    private String schemaStr;
    private String unnamedStr;

    // Single instance of MongoUpdateWritable for result output.
    private MongoUpdateWritable muw;

    /**
     * First constructor
     *
     * @param query  JSON string representing 'query' parameter in MongoDB update
     * @param update JSON string representing 'update' parameter in MongoDB update
     */
    public MongoUpdateStorage(final String query, final String update) {
        this(query, update, null);
    }

    /**
     * Second constructor
     *
     * @param query  JSON string representing 'query' parameter in MongoDB update
     * @param update JSON string representing 'update' parameter in MongoDB update
     * @param schema string representing schema of pig output
     */
    public MongoUpdateStorage(final String query, final String update, final String schema) {
        this(query, update, schema, "");
    }

    /**
     * Third constructor
     *
     * @param query    JSON string representing 'query' parameter in MongoDB update
     * @param update   JSON string representing 'update' parameter in MongoDB update
     * @param schema   string representing schema of pig output
     * @param toIgnore string representing "unnamed" objects
     */
    public MongoUpdateStorage(final String query, final String update, final String schema, final String toIgnore) {
        this(query, update, schema, toIgnore, "");
    }

    /**
     * Third constructor
     *
     * @param query         JSON string representing 'query' parameter in MongoDB update
     * @param update        JSON string representing 'update' parameter in MongoDB update
     * @param schema        string representing schema of pig output
     * @param toIgnore      string representing "unnamed" objects
     * @param updateOptions JSON string representing 'extra' MongoDB update options
     */
    public MongoUpdateStorage(final String query, final String update, final String schema, final String toIgnore,
                              final String updateOptions) {
        pigReplace = new JSONPigReplace(new String[]{query, update, updateOptions});
        schemaStr = schema;
        unnamedStr = toIgnore.isEmpty() ? null : toIgnore;
        muw = new MongoUpdateWritable();
    }

    @Override
    public void checkSchema(final ResourceSchema s) throws IOException {
        schema = s;
        UDFContext udfContext = UDFContext.getUDFContext();

        Properties p = udfContext.getUDFProperties(getClass(), new String[]{signature});
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
            // perform substitution on variables "marked" for replacements
            BasicBSONObject[] toUpdate = pigReplace.substitute(tuple, schema, unnamedStr);
            // 'query' JSON
            BasicBSONObject q = toUpdate[0];
            // 'update' JSON
            BasicBSONObject u = toUpdate[1];

            // multi and upsert 'options' JSON
            boolean isUpsert = true;
            boolean isMulti = false;
            BasicBSONObject mu = toUpdate.length > 2 ? toUpdate[2] : null;
            if (mu != null) {
                isUpsert = !mu.containsField("upsert") || mu.getBoolean("upsert");
                isMulti = mu.containsField("multi") && mu.getBoolean("multi");
            }

            muw.setQuery(q);
            muw.setModifiers(u);
            muw.setUpsert(isUpsert);
            muw.setMultiUpdate(isMulti);
            recordWriter.write(null, muw);
        } catch (Exception e) {
            throw new IOException("Couldn't convert tuple to bson: ", e);
        }
    }

    @Override
    public void prepareToWrite(final RecordWriter writer) throws IOException {
        //noinspection unchecked
        recordWriter = (MongoRecordWriter<?, MongoUpdateWritable>) writer;
        LOG.info("Preparing to write to " + recordWriter);
        if (recordWriter == null) {
            throw new IOException("Invalid Record Writer");
        }

        UDFContext context = UDFContext.getUDFContext();
        Properties p = context.getUDFProperties(getClass(), new String[]{signature});

        /*
         * In determining the schema to use, the user-defined schema should take
         * precedence over the "inferred" schema
         */
        if (schemaStr != null) {
            try {
                schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        } else {
            String s = p.getProperty(SCHEMA_SIGNATURE);
            if (s == null) {
                throw new IOException("Could not find schema in UDF context. You'd have to explicitly specify a Schema.");
            }
            schema = new ResourceSchema(Utils.getSchemaFromString(s));
        }
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return outputFormat;
    }

    @Override
    public String relToAbsPathForStoreLocation(final String location, final Path curDir) throws IOException {
        return location;
    }

    @Override
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
    public void setStoreFuncUDFContextSignature(final String signature) {
        this.signature = signature;
    }
}
