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

import java.util.*;
// Mongo

import org.bson.*;
import com.mongodb.*;
import com.mongodb.hadoop.util.*;
import com.mongodb.hadoop.splitter.MultiMongoCollectionSplitter;
import com.mongodb.hadoop.splitter.MultiCollectionSplitBuilder;

// Hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

/**
 * The treasury yield xml config object.
 */
public class TreasuryYieldMulti extends MongoTool {

    static{
        // Load the XML config defined in hadoop-local.xml
        Configuration.addDefaultResource( "src/examples/hadoop-local.xml" );
        Configuration.addDefaultResource( "src/examples/mongo-defaults.xml" );
    }

    public static void main( final String[] pArgs ) throws Exception{
        TreasuryYieldMulti tym = new TreasuryYieldMulti();

        //Here is an example of how to use multiple collections as the input to
        //a hadoop job, from within Java code directly.
        MultiCollectionSplitBuilder mcsb = new MultiCollectionSplitBuilder();
        mcsb.add(new MongoURI("mongodb://localhost:27017/mongo_hadoop.yield_historical.in"),
                 (MongoURI)null, // authuri
                 true, // notimeout
                 (DBObject)null, // fields
                 (DBObject)null, // sort
                 (DBObject)null, // query
                 false)
            .add(new MongoURI("mongodb://localhost:27017/mongo_hadoop.yield_historical.in"),
                 (MongoURI)null, // authuri
                 true, // notimeout
                 (DBObject)null, // fields
                 (DBObject)null, // sort
                 new BasicDBObject("_id", new BasicDBObject("$gt", new Date(883440000000L))),
                 false); // range query

        Configuration conf = new Configuration();
        conf.set(MultiMongoCollectionSplitter.MULTI_COLLECTION_CONF_KEY, mcsb.toJSON());
        conf.setSplitterClass(conf, MultiMongoCollectionSplitter.class);

        System.exit( ToolRunner.run(conf, new TreasuryYieldXMLConfig(), pArgs ) );
    }
}

