package com.mongodb.hadoop.splitter;

import com.mongodb.DBObject;
import com.mongodb.hadoop.input.MongoInputSplit;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Utilities for testing Splitter classes that produce MongoInputSplits.
 */
public final class MongoSplitterTestUtils {

    private MongoSplitterTestUtils() {}

    /**
     * Assert that a split has the expected bounds using $min/$max.
     * @param split an instance of MongoInputSplit
     * @param min the min bound
     * @param max the max bound
     */
    public static void assertSplitBounds(
      final MongoInputSplit split, final Integer min, final Integer max) {
        assertEquals(min, split.getMin().get("_id"));
        assertEquals(max, split.getMax().get("_id"));

    }

    /**
     * Assert that a split has the expected bounds using a range query.
     * @param split an instance of MongoInputSplit
     * @param min the min bound
     * @param max the max bound
     */
    public static void assertSplitRange(
      final MongoInputSplit split, final Integer min, final Integer max) {
        DBObject queryObj = (DBObject) split.getQuery().get("_id");
        assertEquals(min, queryObj.get("$gte"));
        assertEquals(max, queryObj.get("$lt"));
    }

    /**
     * Assert that a list of splits has the expected overall count.
     * @param expected the expected count
     * @param splits a list of MongoInputSplits
     */
    public static void assertSplitsCount(
      final long expected, final List<InputSplit> splits) {
        int splitTotal = 0;
        for (InputSplit split : splits) {
            // Cursors have been closed; create a copy of the MongoInputSplit.
            MongoInputSplit mis = new MongoInputSplit((MongoInputSplit) split);
            // Query doesn't play nice with min/max, so use itcount for test.
            splitTotal += mis.getCursor().itcount();
        }
        assertEquals(expected, splitTotal);
    }

}
