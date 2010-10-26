package com.mongodb.hadoop;

import java.io.DataInput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.*;


import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;

import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Configuration helper tool for MongoDB related Map/Reduce jobs
 * Instance based, more idiomatic for those who prefer it to the static methoding of ConfigUtil
 **/

public class MongoConfig {
    private static final Log log = LogFactory.getLog(MongoConfig.class);

    final Configuration _conf;

    @Deprecated
    /** you probably don't want to use this. */
    public MongoConfig() {
        _conf = new Configuration();
    }

    public MongoConfig(Configuration conf) {
        _conf = conf;
    }

    public MongoConfig(DataInput in) throws IOException {
        _conf = new Configuration();
        _conf.readFields(in);
    }

    public boolean isJobVerbose() {
        return MongoConfigUtil.isJobVerbose(_conf);
    }

    public void setJobVerbose(boolean val) {
        MongoConfigUtil.setJobVerbose(_conf, val);
    }

    public boolean isJobBackground() {
        return MongoConfigUtil.isJobBackground(_conf);
    }

    public void setJobBackground( boolean val) {
        MongoConfigUtil.setJobBackground(_conf, val);
    }

    public Class<? extends Mapper> getMapper() {
        return MongoConfigUtil.getMapper(_conf);
    }

    public void setMapper(Class<? extends Mapper> val) {
        MongoConfigUtil.setMapper(_conf, val);
    }

    public Class<?> getMapperOutputKey() {
        return MongoConfigUtil.getMapperOutputKey(_conf);
    }

    public void setMapperOutputKey(Class<?> val) {
        MongoConfigUtil.setMapperOutputKey(_conf, val);
    }

    public Class<?> getMapperOutputValue() {
        return MongoConfigUtil.getMapperOutputValue(_conf);
    }

    public void setMapperOutputValue(Class<?> val) {
        MongoConfigUtil.setMapperOutputKey(_conf, val);
    }

    public Class<? extends Reducer> getCombiner() {
        return MongoConfigUtil.getCombiner(_conf);
    }

    public void setCombiner(Class<? extends Reducer> val) {
        MongoConfigUtil.setCombiner(_conf, val);
    }

    public Class<? extends Reducer> getReducer() {
        return MongoConfigUtil.getReducer(_conf);
    }

    public void setReducer(Class<? extends Reducer> val) {
        MongoConfigUtil.setReducer(_conf, val);
    }


    public Class<? extends Partitioner> getPartitioner() {
        return MongoConfigUtil.getPartitioner(_conf);
    }

    public void setPartitioner(Class<? extends Partitioner> val) {
        MongoConfigUtil.setPartitioner(_conf, val);
    }

    public Class<? extends RawComparator> getSortComparator() {
        return MongoConfigUtil.getSortComparator(_conf);
    }

    public void setSortComparator(Class<? extends RawComparator> val) {
        MongoConfigUtil.setSortComparator(_conf, val);
    }

    public Class<? extends OutputFormat> getOutputFormat() {
        return MongoConfigUtil.getOutputFormat(_conf);
    }

    public void setOutputFormat(Class<? extends OutputFormat> val) {
        MongoConfigUtil.setOutputFormat(_conf, val);
    }


    public Class<?> getOutputKey() {
        return MongoConfigUtil.getOutputKey(_conf);
    }

    public void setOutputKey(Class<?> val) {
        MongoConfigUtil.setOutputKey(_conf, val);
    }

    public Class<?> getOutputValue() {
        return MongoConfigUtil.getOutputValue(_conf);
    }

    public void setOutputValue(Class<?> val) {
        MongoConfigUtil.setOutputValue(_conf, val);
    }

    public Class<? extends InputFormat> getInputFormat() {
        return MongoConfigUtil.getInputFormat(_conf);
    }

    public void setInputFormat(Class<? extends InputFormat> val) {
        MongoConfigUtil.setInputFormat(_conf, val);
    }

    public MongoURI getMongoURI(String key) {
        return MongoConfigUtil.getMongoURI(_conf, key);
    }

    public MongoURI getInputURI() {
        return MongoConfigUtil.getInputURI(_conf);
    }

    public DBCollection getOutputCollection() {
        return MongoConfigUtil.getOutputCollection(_conf);
    }

    public DBCollection getInputCollection() {
        return MongoConfigUtil.getInputCollection(_conf);
    }

    public void setMongoURI(String key, MongoURI value) {
        MongoConfigUtil.setMongoURI(_conf, key, value);
    }

    public void setMongoURIString(String key, String value) {
        MongoConfigUtil.setMongoURIString(_conf, key, value);
    }

    public void setInputURI(String uri) {
        MongoConfigUtil.setInputURI(_conf, uri);
    }

    public void setInputURI(MongoURI uri) {
        MongoConfigUtil.setInputURI(_conf, uri);
    }

    public MongoURI getOutputURI() {
        return MongoConfigUtil.getOutputURI(_conf);
    }

    public void setOutputURI(String uri) {
        MongoConfigUtil.setOutputURI(_conf, uri);
    }

    public void setOutputURI(MongoURI uri) {
        MongoConfigUtil.setOutputURI(_conf, uri);
    }

    /**
     * Set JSON but first validate it's parseable into a BSON Object
     */
    public void setJSON(String key, String value) {
        MongoConfigUtil.setJSON(_conf, key, value);
    }

    public DBObject getDBObject(String key) {
        return MongoConfigUtil.getDBObject(_conf, key);
    }

    public void setDBObject(String key, DBObject value) {
        MongoConfigUtil.setDBObject(_conf, key, value);
    }


    public void setQuery(String query) {
        MongoConfigUtil.setQuery(_conf, query);
    }

    public void setQuery( DBObject query) {
        MongoConfigUtil.setQuery(_conf, query);
    }

    public DBObject getQuery() {
        return MongoConfigUtil.getQuery(_conf);
    }


    public void setFields( String fields) {
        MongoConfigUtil.setFields(_conf, fields);
    }

    public void setFields( DBObject fields) {
        MongoConfigUtil.setFields(_conf, fields);
    }

    public DBObject getFields() {
        return MongoConfigUtil.getFields(_conf);
    }

    public void setSort(String sort) {
        MongoConfigUtil.setSort(_conf, sort);
    }

    public void setSort(DBObject sort) {
        MongoConfigUtil.setSort(_conf, sort);
    }

    public DBObject getSort() {
        return MongoConfigUtil.getSort(_conf);
    }

    public int getLimit() {
        return MongoConfigUtil.getLimit(_conf);
    }

    public void setLimit(int limit) {
        MongoConfigUtil.setLimit(_conf, limit);
    }

    public int getSkip() {
        return MongoConfigUtil.getSkip(_conf);
    }

    public void setSkip(int skip) {
        MongoConfigUtil.setSkip(_conf, skip);
    }

    public int getSplitSize() {
        return MongoConfigUtil.getSplitSize(_conf);
    }

    public void setSplitSize(int value) {
        MongoConfigUtil.setSplitSize(_conf, value);
    }

}

