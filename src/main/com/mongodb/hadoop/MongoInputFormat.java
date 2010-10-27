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
import com.mongodb.hadoop.util.MongoIDRange;

import com.mongodb.util.JSON;

public class MongoInputFormat extends InputFormat<Object,BSONObject> {
    private static final Log log = LogFactory.getLog(MongoInputFormat.class);
    private static final DBObject idTrue = new BasicDBObject("_id", 1);
    private static final DBObject idFalse = new BasicDBObject("_id", -1);
    private static final DBObject emptyObj = new BasicDBObject();

    public RecordReader<Object, BSONObject> createRecordReader(InputSplit split, TaskAttemptContext context){
        if ( ! ( split instanceof MongoInputSplit ) )
            throw new IllegalStateException("Creation of a new RecordReader requires a MongoInputSplit instance." );

        MongoInputSplit mis = (MongoInputSplit)split;

        return new MongoRecordReader( mis );
    }

    protected MongoIDRange findMinMax(DBCollection coll) {
        return findMinMax(coll, null);
    }

    protected MongoIDRange findMinMax(DBCollection coll, MongoIDRange currentSegment) {
        DBObject qSpec = null;
        if (currentSegment == null) {
            log.trace("Null segment so looking for whole range.");
            qSpec = emptyObj;
        }
        else {
            BasicDBObjectBuilder b = BasicDBObjectBuilder.start("$query", "");
            b.add("$min", new BasicDBObject("_id", currentSegment.getMin()));
            b.add("$max", new BasicDBObject("_id", currentSegment.getMax()));
            qSpec = b.get();
        }

            //throw new IllegalArgumentException("No data found matching specified query '" + JSON.serialize(conf.getQuery()) + "'.");
        /**
         * To support custom keys like compounds we CANNOT grab _id as a 'ObjectId'...
         */
        log.info("Q Spec: " + qSpec); 
        Object min = coll.find(qSpec, idTrue).sort(idTrue).next().get("_id");
        Object max = coll.find(qSpec, idTrue).sort(idFalse).next().get("_id");
        assert(min != max) : "Minimum range object should not be the same as maximum range object";
        /** 
         * Note we are calling size(), which accounts for limit/skip.
         * count() ignores them.
         **/
        int cursorSize = coll.find().size(); 
        if (cursorSize <= 0) 
            return MongoIDRange.empty;
        MongoIDRange newSegment = new MongoIDRange(min, max, cursorSize);
        log.debug("Found an ID Range: " + newSegment);
        return newSegment;
    }
    
    public List<InputSplit> getSplits(JobContext context) {
        MongoConfig conf = new MongoConfig(context.getConfiguration());

        if (conf.getLimit() > 0 || conf.getSkip() > 0) {
            /** TODO - If they specify skip or limit we create only one input split */
            throw new IllegalArgumentException("skip() and limit() is not currently supported do to input split issues.");
        } 
        else {
            /**
             * On the jobclient side we want *ONLY* the min and max ids
             * for each split;  Actual querying will be done on the individual
             * mappers.
             */
            int splitSize = conf.getSplitSize();
            MongoIDRange range = findMinMax(conf.getInputCollection());
            // For first release, no splits
            /*int numSplits = (range.size() / splitSize + 1);
            
            log.info("Retrieved and configured an outer Mongo IDRange: " + range + " of size " + range.size() + " (after limit/skip ad nauseum).  Setting splitSize to " + splitSize + " Iterating to " + numSplits);
*/
            // TODO - Configure batchSize based off splitSize? Will this give us a more efficient cursor handle?
            /**
             * At the moment using a single cursor,
             * with larger collections we could probably use
             * branched futures threads for walking down the split tree.
             *
             * TODO - Optimize Me &amp; support sharding!
             */
            List<InputSplit> splits = new ArrayList<InputSplit>(1);
            // For first release, no splits, no sharding
            splits.add(new MongoInputSplit(range, conf.getInputURI(), conf.getQuery(), conf.getFields(), conf.getSort(), conf.getLimit(), conf.getSkip()));
            log.info("Calculated " + splits.size() + " split objects.");
            return splits;
        }
    }

        
    public boolean verifyConfiguration(Configuration conf) {
        return true;
    }
}


