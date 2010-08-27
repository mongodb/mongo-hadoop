// MongoConfig.java

package com.mongodb.hadoop;

import java.io.*;
import java.util.*;

import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

public class MongoConfig {

    public static final String C_QUERY = "MONGO_QUERY";
    public static final String C_FIELDS = "MONGO_FIELDS";
    public static final String C_SORT = "MONGO_SORT";
    public static final String C_LIMIT = "MONGO_LIMIT";
    public static final String C_SKIP = "MONGO_SKIP";


    public static final String INPUT = "INPUT";
    public static final String OUTPUT = "OUTPUT";
    
    public static String fieldHost( String type ){
        return "MONGO_" + type + "_HOST";
    }
    
    public static String fieldDB( String type ){
        return "MONGO_" + type + "_DB";
    }

    public static String fieldCollection( String type ){
        return "MONGO_" + type + "_COLLECTION";
    }

    public final static void setInput( Configuration conf , String host , String db , String collection ){
        conf.set( fieldHost( INPUT ) , host );
        conf.set( fieldDB( INPUT ) , db );
        conf.set( fieldCollection( INPUT ) , collection );
    }

    public final static void setOutput( Configuration conf , String host , String db , String collection ){
        conf.set( fieldHost( OUTPUT ) , host );
        conf.set( fieldDB( OUTPUT ) , db );
        conf.set( fieldCollection( OUTPUT ) , collection );
    }
    

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
        _host = context.getConfiguration().get( fieldHost( type ) , "localhost" );
        _db = context.getConfiguration().get( fieldDB( type ) );
        _collection = context.getConfiguration().get( fieldCollection( type ) );

        _limit = context.getConfiguration().getInt( "MONGO_LIMIT" , 0 );

        if ( _db == null )
            throw new IllegalArgumentException( "no db specified for " + type );
        if ( _collection == null )
            throw new IllegalArgumentException( "no collection specified for " + type );

        _mongo = _getMongo( _host );
    }
    
    public MongoConfig( DataInput in )
        throws IOException {
        _host = in.readUTF();
        _db = in.readUTF();
        _collection = in.readUTF();
        _limit = in.readInt();

        _mongo = _getMongo( _host );
    }

    public void write(DataOutput out)
        throws IOException {
        out.writeUTF( _host );
        out.writeUTF( _db );
        out.writeUTF( _collection );
        out.writeInt( _limit );
    }    

    public DBCollection collection(){
        return _mongo.getDB( _db ).getCollection( _collection );
    }

    public int limit(){
        return _limit;
    }

    final String _host;
    final String _db;
    final String _collection;

    final int _limit;

    final Mongo _mongo;
    
    static Mongo _getMongo( String host ){
        try {
            return Mongo.getStaticMongo( host );
        }
        catch ( Exception e ){
            throw new RuntimeException( "can't make new mongo" , e );
        }
    }

}
