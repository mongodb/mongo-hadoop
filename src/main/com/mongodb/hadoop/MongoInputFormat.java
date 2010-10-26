// MongoImportFormat.java

package com.mongodb.hadoop;

import java.io.*;
import java.util.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.bson.*;

import org.bson.types.ObjectId;
import com.mongodb.*;

import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.util.MongoConfigUtil;

import com.mongodb.util.JSON;

public class MongoInputFormat extends InputFormat<Object,BSONObject> {
    private static final Log log = LogFactory.getLog(MongoInputFormat.class);

    public RecordReader<Object, BSONObject> createRecordReader(InputSplit split, TaskAttemptContext context){
        if ( ! ( split instanceof MongoInputSplit ) )
            throw new IllegalStateException("Creation of a new RecordReader requires a MongoInputSplit instance." );

        MongoInputSplit mis = (MongoInputSplit)split;

        return new MongoRecordReader( mis );
    }
    
    public List<InputSplit> getSplits(JobContext context) {
        MongoConfig conf = new MongoConfig(context.getConfiguration());

        /**
         * On the jobclient side we want *ONLY* the id
         * configged Fields is for the mapper/recordreader to handle 
         */
        DBCursor cursor = conf.getInputCollection().find(conf.getQuery(), new BasicDBObject("_id", 1));
        cursor = cursor.sort(conf.getSort());
        log.debug("Cursor created: " + cursor);
        // TODO - Skip first or limit first?
        cursor = cursor.skip(conf.getSkip());
        cursor = cursor.limit(conf.getLimit());

        /** 
         * Note we are calling size(), which accounts for limit/skip.
         * count() ignores them.
         **/
        int cursorSize = cursor.size(); 
        int splitSize = conf.getSplitSize();

        if (cursorSize <= 0) 
            throw new IllegalArgumentException("No data found matching specified query '" + JSON.serialize(conf.getQuery()) + "'.");
        
        log.info("Retrieved and configured a Mongo cursor of size " + cursorSize + " (after limit/skip ad nauseum).  Setting splitSize to " + splitSize + " Iterating to " + (cursorSize / splitSize + 1));

        // TODO - Configure batchSize based off splitSize? Will this give us a more efficient cursor handle?
        /**
         * At the moment with a single cursor,
         * creating threads &amp; futures makes no sense.
         * We either need to do more logical splits w/ skip &amp; limit
         * or reserve sequential reads for sharding.
         * TODO - Optimize Me!
         */
        List<InputSplit> l = new ArrayList<InputSplit>();
        /**
         * Sane flow control by creating a requisite number of 
         * future jobs as futureSplitters.
         * WARNING: In large splits this may be REALLY bad. 
         */
        for (int i = 0; i < (cursorSize / splitSize + 1); i++) {
            int startSeen = cursor.numSeen();
            log.info("Split Iter " + i);
            HashSet<ObjectId> mongoIDs = new HashSet<ObjectId>(splitSize);
            for (DBObject obj : cursor)  {
                log.info("Input object: " + obj);
                mongoIDs.add((ObjectId) obj.get("_id"));
            }
            int seenItems = startSeen - cursor.numSeen();
            assert(seenItems == mongoIDs.size()) : "Missing items. Expected to have loaded " + seenItems + " but loaded " + mongoIDs.size();
            l.add(new MongoInputSplit(mongoIDs, conf.getInputURI(), conf.getQuery(), conf.getFields(), conf.getSort(), conf.getLimit(), conf.getSkip()));
        }
        log.info("Calculated " + l.size() + " split objects.");
        return l;
    }

        
    public boolean verifyConfiguration(Configuration conf) {
        return true;
    }
}


