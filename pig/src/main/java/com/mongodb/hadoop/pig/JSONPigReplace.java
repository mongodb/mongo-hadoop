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
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * Defines pretty simplified and MongoDB-like
 * Domain Specific Language that helps with updating
 * documents in a collection.
 * 
 * Can be used to specify 'query', 'update', and 'options'
 * in a MongoDB update operation on a collection.
 * 
 */
public class JSONPigReplace {
    // create logger for use, in debugging 
    private static final Log LOG = LogFactory.getLog(JSONPigReplace.class);

    // BSON representing initStrs
    private final BasicBSONObject[] initBSONs;

    // string used to represent pig object that should be 'unnamed' 
    private String unnamedStr;
    // Map of keys to replace in initStr to objects they represent
    private HashMap<String, Object> reps;

    /*
     * @param String[] str :  Array of BSONObject JSON String representations 
     *                (potentially with some values to replace --> in the form $elem) 
     */
    public JSONPigReplace(final String[] str) {
        initBSONs = new BasicBSONObject[str.length];

        reps = new HashMap<String, Object>();

        for (int i = 0; i < str.length; i++) {
            initBSONs[i] = (BasicBSONObject) JSON.parse(str[i]);

            // extract all strings that start with a $ in initStr
            try {
                Matcher m = Pattern.compile("\\$(\\w+)").matcher(str[i]);
                while (m.find()) {
                    reps.put(m.group(1), null);
                }
            } catch (Exception e) {
                LOG.error("Error while extracting strings to replace");
            }
        }
    }

    /*
     * Returns result of substituting pig objects in Tuple t into
     * initStr
     * 
     * @param Tuple t : Pig tuple containing pig objects
     * @param Object s : Schema representing Tuple t
     * @param String un : String to represent un-named Schema Fields 
     * 
     * @return Array of BasicBSONObjects that contain all replacements for "marked" strings
     */
    public BasicBSONObject[] substitute(final Tuple t,
                                        final Object s,
                                        final String un) throws Exception {
        unnamedStr = un;

        final ResourceFieldSchema[] fields;
        try {
            final ResourceSchema schema;
            if (s instanceof String) {
                schema = new ResourceSchema(Utils.getSchemaFromString((String) s));
            } else if (s instanceof Schema) {
                schema = new ResourceSchema((Schema) s);
            } else if (s instanceof ResourceSchema) {
                schema = (ResourceSchema) s;
            } else {
                throw new IllegalArgumentException("Schema must be represented either by a string or a Schema object");
            }
            fields = schema.getFields();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Schema Format");
        }

        // Make Tuple t into BSONObject using schema provided and store result in pObj
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        for (int i = 0; i < fields.length; i++) {
            writeField(builder, fields[i], t.get(i));
        }
        // BSONObject that represents Pig Tuple input using Pig Schema
        BasicBSONObject pObj = (BasicBSONObject) builder.get();

        // fill map of replacement strings to corresponding objects to replace these strings with
        fillReplacementMap(pObj);

        // Now, replace replacement strings (of form $elem) with corresponding objects in pObj      
        return replaceAll(initBSONs, reps);
    }

    /*
     * Fills map of replacement strings (reps) with entries that 
     * map replacement strings to corresponding objects to replace these strings with
     * 
     * @param Object pObj : Object representing pig tuple
     */
    private void fillReplacementMap(final Object pObj) throws IOException {
        if (pObj instanceof BasicBSONObject || pObj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> p = (Map<String, Object>) pObj;
            Object val;
            for (String k : p.keySet()) {
                val = p.get(k);

                if (reps.containsKey(k)) {
                    reps.put(k, val);
                }

                // check if 'val' is an array or an embedded BSON document
                else if (val instanceof BasicBSONObject || val instanceof ArrayList) {
                    fillReplacementMap(val);
                }
            }
        } else if (pObj instanceof ArrayList) {
            for (Object o : (ArrayList) pObj) {
                fillReplacementMap(o);
            }
        }
    }

    /*
     * static method to
     * use reps (map of strings to replace to object  -> corresponding replacements)
     * to make replacements in the BSONObject to act on
     * 
     * @param BasicBSONObject[] ins : BSONObjects to make replacements in
     * @param HashMap<String, Object> reps : replacement map 
     * 
     * @return Array of BasicBSONObjects : the result of replacements
     */
    public static BasicBSONObject[] replaceAll(final BasicBSONObject[] ins, final Map<String, Object> reps) {
        // results of replacements
        BasicBSONObject[] res = new BasicBSONObject[ins.length];

        for (int i = 0; i < res.length; i++) {
            BasicBSONObject template = ins[i];
            if (template != null) {
                try {
                    res[i] = replaceAll(template, reps);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
        return res;
    }

    /*
     * static method to
     * use reps (map of strings to replace to object  -> corresponding replacements)
     * to make replacements in the BSONObject to act on
     * 
     * @param BasicBSONObject in : BSONObject to make replacements in
     * @param HashMap<String, Object> reps : replacement map  
     * 
     * @return BasicBSONObject : result of replacing "marked" strings specified in reps
     */
    public static BasicBSONObject replaceAll(final BasicBSONObject in, final Map<String, Object> reps) {
        if (in == null) {
            throw new IllegalArgumentException("JSON/BasicBSONObject to make substitutions in cannot be null!");
        }

        BasicBSONObject res = new BasicBSONObject();
        String k;
        Object v;
        for (Entry<String, Object> e : in.entrySet()) {
            k = e.getKey();
            v = e.getValue();

            // v is a nested BasicBSONObject or an array
            if (v instanceof BasicBSONObject) {
                res.put(k, replaceAll((BasicBSONObject) v, reps));
            } else {
                if (v instanceof String && ((String) v).startsWith("$")) {
                    res.put(k, reps.get(((String) v).substring(1)));
                } else {
                    res.put(k, v);
                }
            }
        }
        return res;
    }

    /*
     * Writes value in field in Object d into builder
     * 
     * @param BasicDBObjectBuilder builder : builds BSON Object
     * @param ResourceFieldSchema field : field in schema 
     */
    private void writeField(final BasicDBObjectBuilder builder,
                            final ResourceFieldSchema field,
                            final Object d) throws Exception {
        // top-level fields should have a name
        if (field == null) {
            throw new IllegalArgumentException("Top-level fields should have a name");
        }

        // convert Object d into BSON format more suited for storage
        Object convertedType = BSONStorage.getTypeForBSON(d, field, unnamedStr);

        if (convertedType instanceof Map) {
            //noinspection unchecked
            for (Entry<String, Object> entry : ((Map<String, Object>) convertedType).entrySet()) {
                builder.add(getRealKey(entry.getKey()), entry.getValue());
            }
        } else {
            builder.add(getRealKey(field.getName()), convertedType);
        }
    }

    private String getRealKey(String key) {
        if (key != null && key.startsWith("underscore_")) {
            return key.replace("underscore", "");
        } else if (key != null) {
            return key;
        } else {
            return "value";
        }
    }
}
