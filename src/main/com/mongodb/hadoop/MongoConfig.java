// MongoConfig.java

package com.mongodb.hadoop;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.bson.*;
import com.mongodb.*;
import com.mongodb.util.JSON;

import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

public class MongoConfig {
    private static final Log log =
      LogFactory.getLog(MongoConfig.class);

    public enum Mode {
        INPUT, OUTPUT
    }

    protected final Configuration _conf;

    public static final String INPUT_URI = "mongo.input.uri";
    public static final String OUTPUT_URI = "mongo.output.uri";
    public static final String INPUT_QUERY = "mongo.input.query";
    public static final String INPUT_FIELDS = "mongo.input.fields";
    public static final String INPUT_SORT = "mongo.input.sort";
    public static final String INPUT_LIMIT = "mongo.input.limit";
    public static final String INPUT_SKIP = "mongo.input.skip";
    public static final String INPUT_SPLIT_SIZE = "mongo.input.split_size";

    
    public MongoConfig( JobContext context , Mode mode){
        _conf = context.getConfiguration();
        switch (mode) {
            case INPUT: 
                log.info("Initializing _configuration for an input mode.");
                _uriString = _conf.get(INPUT_URI);
                break;
            case OUTPUT:
                log.info("Initializing a _configuration for an output mode");
                _uriString = _conf.get(OUTPUT_URI);
                break;
            default:
                throw new IllegalArgumentException("Invalid Operation Mode. Expected INPUT or OUTPUT.");
        }

        _uri = new MongoURI( _uriString );
        
        _limit = getLimit();
        
        try {
            _collection = _uri.connectCollection( _mongos.connect( _uri ) );
        }
        catch ( Exception e ){
            throw new IllegalArgumentException( "bad uri: " + _uriString , e );
        }
    }
    
    public MongoConfig( DataInput in )
        throws IOException {
        _uriString = in.readUTF();
        _uri = new MongoURI( _uriString );

        _limit = in.readInt();

        _conf = null;

        try {
            _collection = _uri.connectCollection( _mongos.connect( _uri ) );
        }
        catch ( Exception e ){
            throw new IllegalArgumentException( "bad uri: " + _uriString , e );
        }
    }

    public void setQuery(String query) {
        // Test that it's a valid DBObject
        log.debug("Setting query data with JSON String '" + query + "'");
        try {
            Object dbObj = JSON.parse(query);
            log.trace("DBObject representation: " + dbObj);
        } 
        catch (Exception e) {
            throw new IllegalArgumentException("Provided query JSON is not representible/parseable as a BSONObject.");
        }
        _conf.set(INPUT_QUERY, query);
    }

    public void setQuery(BSONObject query) {
        try {
            String json = JSON.serialize(query);
            log.debug("Setting query data with serialized JSON String '" + query + "'");
            _conf.set(INPUT_QUERY, json);
        } 
        catch (Exception e) {
            throw new IllegalArgumentException("Provided query BSONObject cannot be properly represented as JSON data.");
        }
    }

    public String getQueryJSON() {
        return _conf.get(INPUT_QUERY, "{}");
    }

    public BSONObject getQuery() {
        return (BSONObject) JSON.parse(getQueryJSON()); 
    }
    

    public void setFields(String query) {
        // Test that it's a valid DBObject
        log.debug("Setting query data with JSON String '" + query + "'");
        try {
            Object dbObj = JSON.parse(query);
            log.trace("DBObject representation: " + dbObj);
        } 
        catch (Exception e) {
            throw new IllegalArgumentException("Provided query JSON is not representible/parseable as a BSONObject.");
        }
        _conf.set(INPUT_FIELDS, query);
    }

    public void setFields(BSONObject query) {
        try {
            String json = JSON.serialize(query);
            log.debug("Setting query data with serialized JSON String '" + query + "'");
            _conf.set(INPUT_FIELDS, json);
        } 
        catch (Exception e) {
            throw new IllegalArgumentException("Provided query BSONObject cannot be properly represented as JSON data.");
        }
    }

    public String getFieldsJSON() {
        return _conf.get(INPUT_FIELDS, "{}");
    }

    public BSONObject getFields() {
        return (BSONObject) JSON.parse(getFieldsJSON()); 
    }
    

    public void setSort(String query) {
        // Test that it's a valid DBObject
        log.debug("Setting query data with JSON String '" + query + "'");
        try {
            Object dbObj = JSON.parse(query);
            log.trace("DBObject representation: " + dbObj);
        } 
        catch (Exception e) {
            throw new IllegalArgumentException("Provided query JSON is not representible/parseable as a BSONObject.");
        }
        _conf.set(INPUT_SORT, query);
    }

    public void setSort(BSONObject query) {
        try {
            String json = JSON.serialize(query);
            log.debug("Setting query data with serialized JSON String '" + query + "'");
            _conf.set(INPUT_SORT, json);
        } 
        catch (Exception e) {
            throw new IllegalArgumentException("Provided query BSONObject cannot be properly represented as JSON data.");
        }
    }

    public String getSortJSON() {
        return _conf.get(INPUT_SORT, "{}");
    }

    public BSONObject getSort() {
        return (BSONObject) JSON.parse(getSortJSON()); 
    }

    public void setLimit(int limit) {
        log.debug("Setting limit to " + limit);
        _conf.setInt(INPUT_LIMIT, limit);
    }

    public int getLimit() {
        return _conf.getInt(INPUT_LIMIT, 0);
    }

    public void setSkip(int skip) {
        log.debug("Setting skip to " + skip);
        _conf.setInt(INPUT_SKIP, skip);
    }

    public int getSkip() {
        return _conf.getInt(INPUT_SKIP, 0);
    }
    
    public void setSplitSize(int size) {
        _conf.setInt(INPUT_SPLIT_SIZE, size);
    }

    public int getSplitSize() {
        return _conf.getInt(INPUT_SPLIT_SIZE, 0);
    }

    public void write(DataOutput out)
        throws IOException {
        out.writeUTF( _uriString );
        out.writeInt( _limit );
    }    

    public DBCollection collection(){
        return _collection;
    }

    public int limit(){
        return _limit;
    }

    final String _uriString;
    final MongoURI _uri;

    final int _limit;

    final DBCollection _collection;

    private final static Mongo.Holder _mongos = new Mongo.Holder();

    @Override
    public String toString() {
      return "{ _uriString: " + _uriString + ", _uri: " + _uri + ", _collection: " + _collection + ", _limit: " + _limit + "}";
    }
}
