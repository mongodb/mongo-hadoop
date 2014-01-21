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
package com.mongodb.hadoop.examples.treasury;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.splitter.MultiCollectionSplitBuilder;
import com.mongodb.hadoop.splitter.MultiMongoCollectionSplitter;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

/**
 * The treasury yield xml config object.
 */
public class TreasuryYieldMulti extends MongoTool {

    static {
        // Load the XML config defined in hadoop-local.xml
        Configuration.addDefaultResource("src/examples/hadoop-local.xml");
        Configuration.addDefaultResource("src/examples/mongo-defaults.xml");
    }

    public static void main(final String[] pArgs) throws Exception {
        //Here is an example of how to use multiple collections as the input to
        //a hadoop job, from within Java code directly.
        MultiCollectionSplitBuilder mcsb = new MultiCollectionSplitBuilder();
        mcsb.add(new MongoURI("mongodb://localhost:27017/mongo_hadoop.yield_historical.in"),
                 (MongoURI) null, // authuri
                 true, // notimeout
                 (DBObject) null, // fields
                 (DBObject) null, // sort
                 (DBObject) null, // query
                 false,
                 null // splitter class, null will let Hadoop-Connector decide.
        ).add(new MongoURI("mongodb://localhost:27017/mongo_hadoop.yield_historical.in"),
                (MongoURI) null, // authuri
                true, // notimeout
                (DBObject) null, // fields
                (DBObject) null, // sort
                new BasicDBObject("_id", new BasicDBObject("$gt", new Date(883440000000L))),
                false, // range query
                null);

        Configuration conf = new Configuration();
        conf.set(MultiMongoCollectionSplitter.MULTI_COLLECTION_CONF_KEY, mcsb.toJSON());

        System.exit(ToolRunner.run(conf, new TreasuryYieldXMLConfig(), pArgs));
    }
}

