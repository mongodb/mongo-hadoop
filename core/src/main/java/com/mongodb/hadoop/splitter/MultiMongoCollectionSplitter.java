
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

package com.mongodb.hadoop.splitter;

import java.util.*;
import com.mongodb.*;
import com.mongodb.hadoop.input.*;
import com.mongodb.hadoop.util.*;
import org.bson.*;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.util.ReflectionUtils;
import java.io.*;

import com.mongodb.util.JSON;

public class MultiMongoCollectionSplitter extends MongoSplitter {

    public static final String MULTI_COLLECTION_CONF_KEY = "mongo.input.multi_uri.json";
    private static final Log log = LogFactory.getLog( MultiMongoCollectionSplitter.class );

    public MultiMongoCollectionSplitter(){ }

    public MultiMongoCollectionSplitter(Configuration conf){
        super(conf);
    }

    @Override
    public List<InputSplit> calculateSplits() throws SplitFailedException{
        List<MongoURI> inputURIs = MongoConfigUtil.getMongoURIs(this.conf, MongoConfigUtil.INPUT_URI);
        List<InputSplit> returnVal = new LinkedList<InputSplit>();
        List<MongoSplitter> splitters = new LinkedList<MongoSplitter>();

        //For each input URI that is specified, get the appropriate
        //splitter for each implementation.
        if(inputURIs.size() > 0){
            log.info("Using global split settings for multiple URIs specified.");
            //Get options from the hadoop config.
            //Use these options for all URIs in the list.

            //Splitter class is ignored here, since it would just be referring
            //to MultiMongoCollectionSplitter here anyway. If the user needs
            //to customize the splitter class, they should use the JSON key for
            //the configuration instead.
            for(MongoURI uri : inputURIs){
                MongoCollectionSplitter splitter;
                Configuration confForThisUri = new Configuration(conf);
                MongoConfigUtil.setInputURI(confForThisUri, uri);
                confForThisUri.set(MongoConfigUtil.MONGO_SPLITTER_CLASS, "");
                splitter = MongoSplitterFactory.getSplitterByStats(uri,
                        confForThisUri);
                splitters.add(splitter);
            }
        }else{
            //Otherwise the user has set options per-collection.
            log.info("Loading multiple input URIs from JSON stored in "+MULTI_COLLECTION_CONF_KEY);
            DBObject multiUriConfig = MongoConfigUtil.getDBObject(this.conf,
                    MULTI_COLLECTION_CONF_KEY);

            if(!(multiUriConfig instanceof List)){
                throw new IllegalArgumentException("Invalid JSON format in multi uri config key: " +
                                                   "Must be an array where each element is an object " +
                                                   "describing the URI and config options for each split.");
            }
            for(Object obj : (List)multiUriConfig){
                Map<String,Object> configMap;
                MongoURI inputURI;
                Configuration confForThisUri;
                try{
                    configMap = (Map<String,Object>)obj;
                    log.info("building config from " +  configMap.toString());
                    confForThisUri = MongoConfigUtil.buildConfiguration(configMap);
                    inputURI = MongoConfigUtil.getInputURI(confForThisUri);
                }catch(ClassCastException e){
                    throw new IllegalArgumentException("Invalid JSON format in multi uri config key: " +
                                                       "each config item must be an object with keys/values " +
                                                       "describing options for each URI.");
                }
                MongoSplitter splitter;
                Class<? extends MongoSplitter> splitterClass = 
                    MongoConfigUtil.getSplitterClass(confForThisUri);

                if(splitterClass != null){
                    log.info("Using custom splitter class for: " + inputURI +
                             " " + splitterClass);
                    //User wants to use a specific custom class for this URI.
                    //Make sure that the custom class isn't this one
                    if(splitterClass == MultiMongoCollectionSplitter.class){
                        throw new IllegalArgumentException("Can't nest uses of MultiMongoCollectionSplitter");
                    }
                    //All clear.
                    MongoCollectionSplitter collectionSplitter;
                    collectionSplitter = (MongoCollectionSplitter)
                        ReflectionUtils.newInstance(splitterClass, confForThisUri);
                    //Since we use no-arg constructor, need to inject
                    //configuration and input URI.
                    collectionSplitter.setConfiguration(confForThisUri);
                    splitter = collectionSplitter;
                }else{
                    log.info("Fetching collection stats for " + inputURI + " " +
                             "to choose splitter implementation.");
                    //No class was specified, so choose one by looking at
                    //collection stats.
                    splitter = MongoSplitterFactory.getSplitterByStats(inputURI, confForThisUri);
                }
                splitters.add(splitter);
            }
        }

        //Now we have splitters for all the input collections.
        //Run through all of em, get all the splits for each,
        //compile them into one big ol' list.
        for(MongoSplitter splitter : splitters){
            returnVal.addAll(splitter.calculateSplits());
        }
        return returnVal;
    }

}
