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
import com.mongodb.hadoop.BSONFileOutputFormat;
import com.mongodb.hadoop.pig.udf.types.PigBoxedBSONValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
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
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class BSONStorage extends StoreFunc implements StoreMetadata {

    private static final Log LOG = LogFactory.getLog(MongoStorage.class);
    static final String SCHEMA_SIGNATURE = "bson.pig.output.schema";
    //CHECKSTYLE:OFF
    protected ResourceSchema schema = null;
    //CHECKSTYLE:ON
    private RecordWriter out;

    private String udfcSignature = null;
    private String idField = null;

    private final BSONFileOutputFormat outputFormat = new BSONFileOutputFormat();

    public BSONStorage() {
    }

    public BSONStorage(final String idField) {
        this.idField = idField;
    }

    /**
     * Returns object more suited for BSON storage. Object o corresponds to a field value in pig.
     *
     * @param o        object representing pig type to convert to BSON-like object
     * @param field    field to place o in
     * @param toIgnore name of field in Object o to ignore
     * @return an Object that can be stored as BSON.
     * @throws IOException if no schema is available from the field
     */
    public static Object getTypeForBSON(final Object o, final ResourceFieldSchema field, final String toIgnore)
      throws IOException {
        byte dataType;
        ResourceSchema fieldInnerSchema = null;
        if (null == field || DataType.UNKNOWN == field.getType()) {
            dataType = DataType.findType(o);
        } else {
            dataType = field.getType();
            fieldInnerSchema = field.getSchema();
        }

        if (dataType == DataType.BYTEARRAY && o instanceof Map) {
            dataType = DataType.MAP;
        }

        switch (dataType) {
            case DataType.NULL:
                return null;
            case DataType.INTEGER:
            case DataType.LONG:
            case DataType.FLOAT:
            case DataType.DOUBLE:
                return o;
            case DataType.BYTEARRAY:
                if (o instanceof PigBoxedBSONValue) {
                    return ((PigBoxedBSONValue) o).getObject();
                }
                return o.toString();
            case DataType.CHARARRAY:
                return o;
            case DataType.DATETIME:
                return ((DateTime) o).toDate();
            //Given a TUPLE, create a Map so BSONEncoder will eat it
            case DataType.TUPLE:
                // If there is no inner schema, just return the Tuple.
                // BasicBSONEncoder will consume it as an Iterable.
                if (fieldInnerSchema == null) {
                    return o;
                }

                // If there was an inner schema, create a Map from the Tuple.
                ResourceFieldSchema[] fs = fieldInnerSchema.getFields();
                // check if fs[0] should be 'unnamed', in which case, we create
                // an array of 'inner' elements.
                // For example, {("a"),("b")} becomes ["a","b"] if/
                // unnamedStr == "t" and schema for bag is {<*>:(t:chararray)}/
                // <*> -> can be any string since the field name of the tuple in
                // a bag should be ignored
                if (1 == fs.length && fs[0].getName().equals(toIgnore)) {
                    return getTypeForBSON(((Tuple) o).get(0), fs[0], toIgnore);
                }
                // If there is more than one field in the tuple or no fields
                // to ignore, treat the Tuple as a Map.
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                for (int j = 0; j < fs.length; j++) {
                    m.put(fs[j].getName(), getTypeForBSON(((Tuple) o).get(j), fs[j], toIgnore));
                }
                return m;
            // Given a BAG, create an Array so BSONEncoder will eat it.
            case DataType.BAG:
                // If there is no inner schema, just return the Bag.
                // BasicBSONEncoder will consume it as an Iterable.
                if (null == fieldInnerSchema) {
                    return o;
                }
                fs = fieldInnerSchema.getFields();
                ArrayList<Object> bagList = new ArrayList<Object>();
                for (Tuple t : (DataBag) o) {
                    bagList.add(getTypeForBSON(t, fs[0], toIgnore));
                }
                return bagList;
            case DataType.MAP:
                if (o == null) {
                    return null;
                }
                Map map = (Map) o;
                Map<String, Object> out = new HashMap<String, Object>(map.size());
                for (Object key : map.keySet()) {
                    out.put(key.toString(), getTypeForBSON(map.get(key), null, toIgnore));
                }
                return out;
            default:
                return o;
        }
    }

    @SuppressWarnings("unchecked")
    protected void writeField(final BasicDBObjectBuilder builder, final ResourceFieldSchema field, final Object d) throws IOException {
        Object convertedType = getTypeForBSON(d, field, null);
        String fieldName = field != null ? field.getName() : "value";

        if (convertedType instanceof Map) {
            for (Map.Entry<String, Object> mapentry : ((Map<String, Object>) convertedType).entrySet()) {
                String addKey = mapentry.getKey().equals(idField) ? "_id" : mapentry.getKey();
                builder.add(addKey, mapentry.getValue());
            }
        } else {
            builder.add(fieldName, convertedType);
        }

    }

    @Override
    public void checkSchema(final ResourceSchema schema) throws IOException {
        this.schema = schema;
        UDFContext context = UDFContext.getUDFContext();

        Properties p = context.getUDFProperties(getClass(), new String[]{udfcSignature});
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
            if (schema != null) {
                fields = schema.getFields();
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

    @Override
    public void prepareToWrite(final RecordWriter writer) throws IOException {
        out = writer;
        if (out == null) {
            throw new IOException("Invalid Record Writer");
        }

        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(getClass(), new String[]{udfcSignature});
        String strSchema = p.getProperty(SCHEMA_SIGNATURE);
        if (strSchema == null) {
            LOG.warn("Could not find schema in UDF context!");
            LOG.warn("Will attempt to write records without schema.");
        }

        try {
            // Parse the schema from the string stored in the properties object.
            schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        } catch (Exception e) {
            schema = null;
            LOG.warn(e.getMessage());
        }

    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return outputFormat;
    }

    @Override
    public String relToAbsPathForStoreLocation(final String location, final Path curDir) throws IOException {
        return LoadFunc.getAbsolutePath(location, curDir);
    }

    @Override
    public void setStoreLocation(final String location, final Job job) throws IOException {
        final Configuration config = job.getConfiguration();
        // Old property.
        config.set("mapred.output.dir", location);
        // Modern property.
        config.set("mapreduce.output.fileoutputformat.outputdir", location);
    }


    @Override
    public void setStoreFuncUDFContextSignature(final String signature) {
        udfcSignature = signature;
    }

}
