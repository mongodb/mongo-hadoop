package com.mongodb.hadoop.util;

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

/**
 * Mongo ID Range holder.
 * Specifies a start '_id' and end '_id', assuming a collection is sorted by '_id'.
 * Note: Does NOT work with ObjectId as that idea is predicated on the concept that the 
 * collection in question is using native Mongo IDs, rather than a custom implementation such as
 * embedded documents.
 *
 * *Immutable*
 *
 * @author Brendan W. McAdams <brendan@10gen.com>
 */

public class MongoIDRange implements Serializable {
    private final Object min;
    private final Object max;
    private final int size;

    public static transient final MongoIDRange empty = new MongoIDRange(null, null, 0);

    public MongoIDRange(Object min, Object max, int size) {
        if (min == null && size != 0)
            throw new IllegalArgumentException("'min' (Minimum ID) must *not* be null.");

        if (max == null && size != 0)
            throw new IllegalArgumentException("'max' (Maximum ID) must *not* be null.");

        this.min = min;
        this.max = max;
        this.size = size;
        
    }

    public Object getMin() { 
        return this.min;
    }

    public Object getMax() {
        return this.max;
    }

    public int size() {
        return this.size;
    }

    @Override
    public String toString() {
        return "{ MongoIDRange('" + this.min + "' ... '" + this.max + "') with " + this.size + " documents. }";
    }
}
