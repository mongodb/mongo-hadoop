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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
     * @param o object representing pig type to convert to BSON-like object
     * @param field field to place o in
     * @param toIgnore name of field in Object o to ignore
     */
    public static Object getTypeForBSON(final Object o, final ResourceSchema.ResourceFieldSchema field, final String toIgnore)
        throws IOException {
        byte dataType = field != null ? field.getType() : DataType.UNKNOWN;
        ResourceSchema s = null;
        if (field == null) {
            if (o instanceof Map) {
                dataType = DataType.MAP;
            } else if (o instanceof List) {
                dataType = DataType.BAG;
            } else {
                dataType = DataType.UNKNOWN;
            }
        } else {
            s = field.getSchema();
            if (dataType == DataType.UNKNOWN) {
                if (o instanceof Map) {
                    dataType = DataType.MAP;
                }
                if (o instanceof List) {
                    dataType = DataType.BAG;
                }
            }
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
                return o.toString();
            case DataType.CHARARRAY:
                return o;

            //Given a TUPLE, create a Map so BSONEncoder will eat it
            case DataType.TUPLE:
                if (s == null) {
                    throw new IOException("Schemas must be fully specified to use this storage function.  No schema found for field "
                                          + field.getName());
                }
                ResourceSchema.ResourceFieldSchema[] fs = s.getFields();
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                for (int j = 0; j < fs.length; j++) {
                    m.put(fs[j].getName(), getTypeForBSON(((Tuple) o).get(j), fs[j], toIgnore));
                }
                return m;

            // Given a BAG, create an Array so BSONEnconder will eat it.
            case DataType.BAG:
                if (s == null) {
                    throw new IOException("Schemas must be fully specified to use this storage function.  No schema found for field "
                                          + field);
                }
                fs = s.getFields();
                if (fs.length != 1 || fs[0].getType() != DataType.TUPLE) {
                    throw new IOException("Found a bag without a tuple inside!");
                }
                // Drill down the next level to the tuple's schema.
                s = fs[0].getSchema();
                if (s == null) {
                    throw new IOException("Schemas must be fully specified to use this storage function.  No schema found for field "
                                          + field.getName());
                }
                fs = s.getFields();
                ArrayList<Object> a = new ArrayList<Object>();

                // check if fs[0] should be 'unnamed', in which case, we create an array
                // of 'inner' elements.
                // For example, {("a"),("b")} becomes ["a","b"] if
                // unnamedStr == "t" and schema for bag is {<*>:(t:chararray)}
                // <*> -> can be any string since the field name of the tuple in a bag should be ignored 
                if (fs.length == 1 && fs[0].getName().equals(toIgnore)) {
                    for (Tuple t : (DataBag) o) {
                        a.add(t.get(0));
                    }
                } else {
                    for (Tuple t : (DataBag) o) {
                        Map<String, Object> ma = new LinkedHashMap<String, Object>();
                        for (int j = 0; j < fs.length; j++) {
                            ma.put(fs[j].getName(), t.get(j));
                        }
                        a.add(ma);
                    }
                }

                return a;
            case DataType.MAP:
                if (o == null) {
                    return o;
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

    protected void writeField(final BasicDBObjectBuilder builder,
                              final ResourceSchema.ResourceFieldSchema field,
                              final Object d) throws IOException {
        Object convertedType = getTypeForBSON(d, field, null);
        String fieldName = field != null ? field.getName() : "value";

        if (convertedType instanceof Map) {
            for (Map.Entry<String, Object> mapentry : ((Map<String, Object>) convertedType).entrySet()) {
                String addKey = mapentry.getKey().equals(this.idField) ? "_id" : mapentry.getKey();
                builder.add(addKey, mapentry.getValue());
            }
        } else {
            builder.add(fieldName, convertedType);
        }

    }

    public void checkSchema(final ResourceSchema schema) throws IOException {
        this.schema = schema;
        UDFContext context = UDFContext.getUDFContext();

        Properties p = context.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty(SCHEMA_SIGNATURE, schema.toString());
    }

    public void storeSchema(final ResourceSchema schema, final String location, final Job job) {
        // not implemented
    }


    public void storeStatistics(final ResourceStatistics stats, final String location, final Job job) {
        // not implemented
    }

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
            LOG.warn("Could not find schema in UDF context!");
            LOG.warn("Will attempt to write records without schema.");
        }

        try {
            // Parse the schema from the string stored in the properties object.
            this.schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        } catch (Exception e) {
            this.schema = null;
            LOG.warn(e.getMessage());
        }

    }

    public OutputFormat getOutputFormat() throws IOException {
        return this.outputFormat;
    }

    public String relToAbsPathForStoreLocation(final String location, final Path curDir) throws IOException {
        return LoadFunc.getAbsolutePath(location, curDir);
    }

    public void setStoreLocation(final String location, final Job job) throws IOException {
        final Configuration config = job.getConfiguration();
        config.set("mapred.output.file", location);
    }


    @Override
    public void setStoreFuncUDFContextSignature(final String signature) {
        udfcSignature = signature;
    }

}
