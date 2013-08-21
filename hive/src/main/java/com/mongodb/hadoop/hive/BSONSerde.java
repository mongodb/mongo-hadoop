package com.mongodb.hadoop.hive;

/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

import java.util.*;

public class BSONSerde implements SerDe {

    private static final Log LOG = LogFactory.getLog(BSONSerde.class.getName());

    /**
     * # of columns in the Hive table
     */
    private int numColumns;
    /**
     * Column names in the Hive table
     */
    private List<String> columnNames;
    /**
     * An ObjectInspector which contains metadata
     * about rows
     */
    private StructObjectInspector rowInspect;

    private ArrayList<Object> row;

    private List<TypeInfo> columnTypes;

    private long serializedSize;
    private SerDeStats stats;
    private boolean lastOperationSerialize;
    private boolean lastOperationDeserialize;

    private boolean test = false;

    public void initialize(Configuration sysProps, Properties tblProps)
                                                throws SerDeException {
        LOG.debug("Initializing BSONSerde");

        /*
         * column names from the Hive table
         */
        String colNameProp = tblProps.getProperty(Constants.LIST_COLUMNS);
        columnNames = Arrays.asList(colNameProp.split(","));

        /*
         * Extract Type Information
         */
        String colTypeProp = tblProps.getProperty(Constants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypeProp);

        assert ( columnNames.size() == columnTypes.size() ) :
                 "Column Names and Types don't match in size";

        numColumns = columnNames.size();

        /*
         * Inspect each column
         */
        List<ObjectInspector> columnInspects = new ArrayList<ObjectInspector>(
                                                    columnNames.size() );
        ObjectInspector inspect;
        for (int c = 0; c < numColumns; c++) {
            inspect = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
                columnTypes.get(c) );
            columnInspects.add( inspect );
        }

        rowInspect = ObjectInspectorFactory.getStandardStructObjectInspector(
                        columnNames, columnInspects);

        /*
         * Create empty row objects which are reused during deser
         */
        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++) {
            row.add(null);
        }

        stats = new SerDeStats();


        LOG.debug("Completed initialization of BSONSerde.");
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowInspect;
    }


    public Object deserialize(Writable blob) throws SerDeException {
        LOG.debug("Deserializing BSON Row with Class: " + blob.getClass());

        BSONObject doc;

        if (blob instanceof BSONWritable) {
            BSONWritable b = (BSONWritable) blob;
            LOG.debug("Got a BSONWritable: " + b);
            doc = b.getDoc();
        } else {
            throw new SerDeException(getClass().toString() +
                    " requires a BSONWritable object, not " + blob.getClass());
        }

        String colName = "";
        Object value;
        for (int c = 0; c < numColumns; c++) {
            try {
                colName = columnNames.get(c);
                TypeInfo ti = columnTypes.get(c);
                String x = "Col #" + c + " Type: " + ti.getTypeName();
                LOG.trace("***" + x);
                // Attempt typesafe casting
                if (!doc.containsField(colName)) {
                    LOG.debug("Cannot find field '" + colName + "' in " + doc.keySet());
                    for (String k : doc.keySet()) {
                        if (k.trim().equalsIgnoreCase(colName)) {
                            colName = k;
                            LOG.debug("K: " + k + "colName: " + colName);
                        } 
                    }
                }
                if (ti.getTypeName().equalsIgnoreCase(
                                Constants.DOUBLE_TYPE_NAME)) {
                    value = (Double) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.BIGINT_TYPE_NAME)) {
                    value = (Long) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.INT_TYPE_NAME)) {
                    value = doc.get(colName);
                    /** Some integers end up stored as doubles
                     * due to quirks of the shell
                     */
                    if (value instanceof  Double)
                        value = ((Double) value).intValue();
                    else
                        value = (Integer) value;
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.TINYINT_TYPE_NAME)) {
                    value = (Byte) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.FLOAT_TYPE_NAME)) {
                    value = (Float) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                        Constants.BOOLEAN_TYPE_NAME)) {
                    value = (Boolean) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.STRING_TYPE_NAME)) {
                    value = (String) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                        Constants.DATE_TYPE_NAME) ||
                        ti.getTypeName().equalsIgnoreCase(
                                Constants.DATETIME_TYPE_NAME)  ||
                        ti.getTypeName().equalsIgnoreCase(
                                Constants.TIMESTAMP_TYPE_NAME)) {
                    value = (Date) doc.get(colName);
                } else if (ti.getTypeName().startsWith(Constants.LIST_TYPE_NAME)) {
                    // Copy to an Object array
                    BasicBSONList lst = (BasicBSONList) doc.get(colName);
                    Object[] arr = new Object[lst.size()];
                    for (int i = 0; i < arr.length; i++) {
                            arr[i] = lst.get(i);
                    }
                    value = arr;
                } else if (ti.getTypeName().startsWith(Constants.MAP_TYPE_NAME)) {
                    // Hack in case embedded doc has _id, acting weird
                    if (doc.containsField("_id")) {
                        if (doc.get("_id") instanceof ObjectId) {
                            doc.put("_id", ((ObjectId) doc.get("_id")).toString());
                        }
                    }
                    value = ((BSONObject) doc).toMap();
                } else if (ti.getTypeName().startsWith(Constants.STRUCT_TYPE_NAME)) {
                    //value = ((BSONObject) doc).toMap();
                    throw new IllegalArgumentException("Unable to currently work with structs.");
                } else {
                    // Fall back, just get an object
                    LOG.warn("FALLBACK ON '" + colName + "'");
                    value = doc.get(colName);
                }
                row.set(c, value);
            } catch (Exception e) {
                LOG.error("Exception decoding row at column " + colName, e);
            }
        }

        LOG.debug("Deserialized Row: " + row);

        return row;
    }

    /**
     * Not sure - something to do with serialization of data
     */
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    public Writable serialize(Object obj, ObjectInspector objInspector)
                                throws SerDeException {
        LOG.info("-----------------------------");
        LOG.info("***** serialize BSON ********");
        LOG.info("-----------------------------");

        return null;
    }

    public SerDeStats getSerDeStats() {
        stats.setRawDataSize(serializedSize);
        return stats;
    }



    private static final byte[] HEX_CHAR = new byte[] {
            '0' , '1' , '2' , '3' , '4' , '5' , '6' , '7' , '8' ,
            '9' , 'A' , 'B' , 'C' , 'D' , 'E' , 'F'
    };

    protected static void dumpBytes( byte[] buffer ){
        StringBuilder sb = new StringBuilder( 2 + ( 3 * buffer.length ) );

        for ( byte b : buffer ){
            sb.append( "0x" ).append( (char) ( HEX_CHAR[( b & 0x00F0 ) >> 4] ) ).append(
                    (char) ( HEX_CHAR[b & 0x000F] ) ).append( " " );
        }

        LOG.info( "Byte Dump: " + sb.toString() );
    }
}
