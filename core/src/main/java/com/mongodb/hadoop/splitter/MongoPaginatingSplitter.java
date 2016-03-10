package com.mongodb.hadoop.splitter;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * MongoPaginatingSplitter is suitable for splitting a collection when given
 * an input query that matches a subset of the documents in the collection,
 * and these documents are spread throughout the index described by
 * {@link com.mongodb.hadoop.util.MongoConfigUtil#INPUT_SPLIT_KEY_PATTERN}.
 * If the documents selected by the query tend to be in a single range of the
 * index, consider using
 * {@link com.mongodb.hadoop.util.MongoConfigUtil#ENABLE_FILTER_EMPTY_SPLITS}
 * instead.
 *
 * Splits produced by this Splitter implementation contain at least
 * {@link com.mongodb.hadoop.util.MongoConfigUtil#INPUT_SPLIT_MIN_DOCS}
 * documents, except for the last split in the collection, which may contain
 * fewer.
 *
 * This Splitter implementation must make several queries to the database in
 * order to build splits.
 */
public class MongoPaginatingSplitter extends MongoCollectionSplitter {

    public MongoPaginatingSplitter() {}

    public MongoPaginatingSplitter(final Configuration conf) {
        super(conf);
    }

    public List<InputSplit> calculateSplits() throws SplitFailedException {
        Configuration conf = getConfiguration();
        if (!MongoConfigUtil.isRangeQueryEnabled(conf)) {
            throw new IllegalArgumentException(
              "Cannot split using " + getClass().getName() + " when "
              + MongoConfigUtil.SPLITS_USE_RANGEQUERY + " is disabled.");
        }
        DBObject splitKeyObj = MongoConfigUtil.getInputSplitKey(conf);
        Set<String> splitKeys = splitKeyObj.keySet();
        if (splitKeys.size() > 1) {
            throw new IllegalArgumentException(
              "Cannot split using " + getClass().getName() + " when "
                + MongoConfigUtil.INPUT_SPLIT_KEY_PATTERN + " describes a "
                + "compound key.");
        }
        String splitKey = splitKeys.iterator().next();
        int minDocs = MongoConfigUtil.getInputSplitMinDocs(conf);
        DBCollection inputCollection =
          MongoConfigUtil.getInputCollection(conf);
        DBObject query = MongoConfigUtil.getQuery(conf);
        DBObject rangeObj = null;
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Object minBound = null, maxBound;
        DBCursor cursor;

        try {
            do {
                if (null == minBound) {
                    cursor = inputCollection.find(query);
                } else {
                    if (null == rangeObj) {
                        rangeObj =
                          new BasicDBObjectBuilder()
                            .push(splitKey).add("$gte", minBound).pop().get();
                        rangeObj.putAll(query);
                    } else {
                        ((DBObject) rangeObj.get(splitKey))
                          .put("$gte", minBound);
                    }
                    cursor = inputCollection.find(rangeObj);
                }
                cursor = cursor.sort(splitKeyObj).skip(minDocs).limit(1);

                if (cursor.hasNext()) {
                    maxBound = cursor.next().get(splitKey);
                } else {
                    maxBound = null;
                }

                BasicDBObject lowerBound = null, upperBound = null;
                if (minBound != null) {
                    lowerBound = new BasicDBObject(splitKey, minBound);
                }
                if (maxBound != null) {
                    upperBound = new BasicDBObject(splitKey, maxBound);
                }
                splits.add(
                  createRangeQuerySplit(lowerBound, upperBound, query));

                minBound = maxBound;
            } while (maxBound != null);
        } finally {
            MongoConfigUtil.close(inputCollection.getDB().getMongo());
        }

        return splits;
    }

}
