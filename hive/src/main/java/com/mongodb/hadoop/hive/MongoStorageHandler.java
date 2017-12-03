/*
 * Copyright 2010-2013 10gen Inc.
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

package com.mongodb.hadoop.hive;

import com.mongodb.DBCollection;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.hive.input.HiveMongoInputFormat;
import com.mongodb.hadoop.hive.output.HiveMongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static com.mongodb.hadoop.hive.BSONSerDe.MONGO_COLS;
import static java.lang.String.format;

/**
 * Used to sync documents in some MongoDB collection with
 * rows in a Hive table
 */
public class MongoStorageHandler extends DefaultStorageHandler
  implements HiveStoragePredicateHandler {
    // stores the location of the collection
    public static final String MONGO_URI = "mongo.uri";
    // get location of where meta-data is stored about the mongo collection
    public static final String TABLE_LOCATION = "location";
    // location of properties file
    public static final String PROPERTIES_FILE_PATH = "mongo.properties.path";
    private Properties properties = null;

    private static final Log LOG = LogFactory.getLog(MongoStorageHandler.class);

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass() {
        return HiveMongoInputFormat.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new MongoHiveMetaHook();
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() {
        return HiveMongoOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return BSONSerDe.class;
    }

    private Properties getProperties(
      final Configuration conf, final String path) throws IOException {
        if (properties == null) {
            properties =
              MongoConfigUtil.readPropertiesFromFile(conf, path);
        }
        return properties;
    }

    @Override
    public DecomposedPredicate decomposePredicate(
      final JobConf jobConf,
      final Deserializer deserializer,
      final ExprNodeDesc predicate) {
        BSONSerDe serde = (BSONSerDe) deserializer;

        // Create a new analyzer capable of handling equality and general
        // binary comparisons (false = "more than just equality").
        // TODO: The analyzer is only capable of handling binary comparison
        // expressions, but we could push down more than that in the future by
        // writing our own analyzer.
        IndexPredicateAnalyzer analyzer =
          IndexPredicateAnalyzer.createAnalyzer(false);
        // Predicate may contain any column.
        for (String colName : serde.columnNames) {
            analyzer.allowColumnName(colName);
        }
        List<IndexSearchCondition> searchConditions =
          new LinkedList<IndexSearchCondition>();
        ExprNodeDesc residual = analyzer.analyzePredicate(
          predicate, searchConditions);

        DecomposedPredicate decomposed = new DecomposedPredicate();
        decomposed.pushedPredicate =
          analyzer.translateSearchConditions(searchConditions);
        decomposed.residualPredicate = (ExprNodeGenericFuncDesc) residual;
        return decomposed;
    }

    /**
     * HiveMetaHook used to define events triggered when a hive table is
     * created and when a hive table is dropped.
     */
    private class MongoHiveMetaHook implements HiveMetaHook {
        @Override
        public void preCreateTable(final Table tbl) throws MetaException {
            Map<String, String> tblParams = tbl.getParameters();
            if (!(tblParams.containsKey(MONGO_URI)
              || tblParams.containsKey(PROPERTIES_FILE_PATH))) {
                throw new MetaException(
                  format("You must specify '%s' or '%s' in TBLPROPERTIES",
                    MONGO_URI, PROPERTIES_FILE_PATH));
            }
        }

        @Override
        public void commitCreateTable(final Table tbl) throws MetaException {
        }

        @Override
        public void rollbackCreateTable(final Table tbl) throws MetaException {
        }

        @Override
        public void preDropTable(final Table tbl) throws MetaException {
        }

        @Override
        public void commitDropTable(final Table tbl, final boolean deleteData) throws MetaException {
            boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

            if (deleteData && !isExternal) {
                Map<String, String> tblParams = tbl.getParameters();
                DBCollection coll;
                if (tblParams.containsKey(MONGO_URI)) {
                    String mongoURIStr = tblParams.get(MONGO_URI);
                    coll = MongoConfigUtil.getCollection(
                      new MongoClientURI(mongoURIStr));
                } else if (tblParams.containsKey(PROPERTIES_FILE_PATH)) {
                    String propertiesPathStr =
                      tblParams.get(PROPERTIES_FILE_PATH);
                    Properties properties;
                    try {
                        properties =
                          getProperties(getConf(), propertiesPathStr);
                    } catch (IOException e) {
                        throw new MetaException(
                          "Could not read properties file "
                            + propertiesPathStr + ". Reason: " + e.getMessage());
                    }
                    if (!properties.containsKey(MONGO_URI)) {
                        throw new MetaException(
                          "No URI given in properties file: "
                            + propertiesPathStr);
                    }
                    String uriString = properties.getProperty(MONGO_URI);
                    coll = MongoConfigUtil.getCollection(
                      new MongoClientURI(uriString));
                } else {
                    throw new MetaException(
                      format(
                        "Could not find properties '%s' or '%s'. "
                          + "At least one must be defined. "
                          + "Collection not dropped.",
                        MONGO_URI, PROPERTIES_FILE_PATH));
                }
                try {
                    coll.drop();
                } finally {
                    MongoConfigUtil.close(coll.getDB().getMongo());
                }
            }
        }

        @Override
        public void rollbackDropTable(final Table tbl) throws MetaException {
        }
    }

    @Override
    public void configureInputJobProperties(final TableDesc tableDesc, final Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        copyJobProperties(properties, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(final TableDesc tableDesc, final Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        copyJobProperties(properties, jobProperties);
    }

    /**
     * Helper function to copy properties
     */
    private void copyJobProperties(final Properties from, final Map<String, String> to) {
        // Copy Hive-specific properties used directly by
        // HiveMongoInputFormat, BSONSerDe.
        if (from.containsKey(serdeConstants.LIST_COLUMNS)) {
            to.put(serdeConstants.LIST_COLUMNS,
                    (String) from.get(serdeConstants.LIST_COLUMNS));
        }
        if (from.containsKey(serdeConstants.LIST_COLUMN_TYPES)) {
            to.put(serdeConstants.LIST_COLUMN_TYPES,
                    (String) from.get(serdeConstants.LIST_COLUMN_TYPES));
        }
        if (from.containsKey(MONGO_COLS)) {
            to.put(MONGO_COLS, (String) from.get(MONGO_COLS));
        }
        if (from.containsKey(TABLE_LOCATION)) {
            to.put(TABLE_LOCATION, (String) from.get(TABLE_LOCATION));
        }

        // First, merge properties from the given properties file, if there
        // was one. These can be overwritten by other table properties later.
        String propertiesFilePathString =
          from.getProperty(PROPERTIES_FILE_PATH);
        if (propertiesFilePathString != null) {
            try {
                Properties properties =
                  getProperties(getConf(), propertiesFilePathString);
                for (Map.Entry<Object, Object> prop : properties.entrySet()) {
                    String key = (String) prop.getKey();
                    String value = (String) prop.getValue();
                    if (key.equals(MONGO_URI)) {
                        // Copy to input/output URI.
                        to.put(MongoConfigUtil.INPUT_URI, value);
                        to.put(MongoConfigUtil.OUTPUT_URI, value);
                    } else {
                        to.put(key, value);
                    }
                }
            } catch (IOException e) {
                LOG.error(
                  "Error while trying to read properties file "
                    + propertiesFilePathString, e);
            }
        }

        // Copy general connector properties, such as ones defined in
        // MongoConfigUtil. These are all prefixed with "mongo.".
        for (Entry<Object, Object> entry : from.entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith("mongo.")) {
                to.put(key, (String) from.get(key));
            }
        }

        // Update the keys for MONGO_URI per MongoConfigUtil.
        if (from.containsKey(MONGO_URI)) {
            String mongoURIStr = (String) from.get(MONGO_URI);
            to.put(MongoConfigUtil.INPUT_URI, mongoURIStr);
            to.put(MongoConfigUtil.OUTPUT_URI, mongoURIStr);
        }
    }
}
