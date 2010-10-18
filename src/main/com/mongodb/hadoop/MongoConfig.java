// MongoConfig.java

package com.mongodb.hadoop;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

public class MongoConfig {
  private static final Log log =
    LogFactory.getLog(MongoConfig.class);


    static final String INPUT = "INPUT";
    static final String OUTPUT = "OUTPUT";

    public static final String C_INPUT = "MONGO_" + INPUT;
    public static final String C_OUTPUT = "MONGO_" + OUTPUT;
    
    public static final String C_QUERY = "MONGO_QUERY";
    public static final String C_FIELDS = "MONGO_FIELDS";
    public static final String C_SORT = "MONGO_SORT";
    public static final String C_LIMIT = "MONGO_LIMIT";
    public static final String C_SKIP = "MONGO_SKIP";

    public final static void setQuery( Configuration conf , BSONObject query , BSONObject fields , BSONObject sort , int limit , int skip ){
        _set( conf , C_QUERY , query );
        _set( conf , C_FIELDS , fields );
        _set( conf , C_SORT , sort );

        if ( limit > 0 )
            conf.setInt( C_LIMIT , limit );
        if ( skip > 0 )
            conf.setInt( C_SKIP , skip );

    }
    
    static void _set( Configuration conf , String name , BSONObject o ){
        if ( o == null )
            return;

        throw new RuntimeException( "not implemented yet" );
    }

    public MongoConfig( JobContext context , String type ){
        _uriString = context.getConfiguration().get( "MONGO_" + type );
        _uri = new MongoURI( _uriString );
        
        _limit = context.getConfiguration().getInt( "MONGO_LIMIT" , 0 );
        
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

        try {
            _collection = _uri.connectCollection( _mongos.connect( _uri ) );
        }
        catch ( Exception e ){
            throw new IllegalArgumentException( "bad uri: " + _uriString , e );
        }
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
