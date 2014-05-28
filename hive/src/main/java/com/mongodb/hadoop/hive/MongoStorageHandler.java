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
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/*
 * Used to sync documents in some MongoDB collection with
 * rows in a Hive table
 */
public class MongoStorageHandler extends DefaultStorageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MongoStorageHandler.class);
    
    // stores the location of the collection
    public static final String MONGO_URI = "mongo.uri";
    // get location of where meta-data is stored about the mongo collection
    public static final String TABLE_LOCATION = "location";

    public MongoStorageHandler() {
    }

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
    public Class<? extends SerDe> getSerDeClass() {
        return BSONSerDe.class;
    }

    /*
     * HiveMetaHook used to define events triggered when a hive table is
     * created and when a hive table is dropped.
     */
    private class MongoHiveMetaHook implements HiveMetaHook {
        @Override
        public void preCreateTable(final Table tbl) throws MetaException {
            Map<String, String> tblParams = tbl.getParameters();
            if (!tblParams.containsKey(MONGO_URI)) {
                throw new MetaException("You must specify 'mongo.uri' in TBLPROPERTIES");
            }
        }

        @Override
        //CHECKSTYLE:OFF
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
        //CHECKSTYLE:ON
            boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

            if (deleteData && !isExternal) {
                Map<String, String> tblParams = tbl.getParameters();
                if (tblParams.containsKey(MONGO_URI)) {
                    String mongoURIStr = tblParams.get(MONGO_URI);
                    DBCollection coll = MongoConfigUtil.getCollection(new MongoClientURI(mongoURIStr));
                    coll.drop();
                } else {
                    throw new MetaException("No 'mongo.uri' property found. Collection not dropped.");
                }
            }
        }

        @Override
        //CHECKSTYLE:OFF
        public void rollbackDropTable(final Table tbl) throws MetaException {
        //CHECKSTYLE:ON
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

    /*
     * Helper function to copy properties
     */
    private void copyJobProperties(final Properties from, final Map<String, String> to) {
        for (Entry<Object, Object> e : from.entrySet()) {
            to.put((String) e.getKey(), (String) e.getValue());
        }

        if (to.containsKey(MONGO_URI)) {
            String mongoURIStr = to.get(MONGO_URI);
            to.put(MongoConfigUtil.INPUT_URI, mongoURIStr);
            to.put(MongoConfigUtil.OUTPUT_URI, mongoURIStr);
        }
    }
}
