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

    public static final String INPUT = "INPUT";
    public static final String OUTPUT = "OUTPUT";
    
    public static String fieldDB( String type ){
        return "MONGO_" + type + "_DB";
    }

    public static String fieldCollection( String type ){
        return "MONGO_" + type + "_COLLECTION";
    }

    public final static void setInput( Configuration conf , String db , String collection ){
        conf.set( fieldDB( INPUT ) , db );
        conf.set( fieldCollection( INPUT ) , collection );
    }

    public final static void setOutput( Configuration conf , String db , String collection ){
        conf.set( fieldDB( OUTPUT ) , db );
        conf.set( fieldCollection( OUTPUT ) , collection );
    }

    public MongoConfig( JobContext context , String type ){
        
        _db = context.getConfiguration().get( fieldDB( type ) );
        _collection = context.getConfiguration().get( fieldCollection( type ) );
        
        if ( _db == null )
            throw new IllegalArgumentException( "no db specified for " + type );
        if ( _collection == null )
            throw new IllegalArgumentException( "no collection specified for " + type );

        _mongo = _getMongo();
    }
    
    public MongoConfig( DataInput in )
        throws IOException {
        _db = in.readUTF();
        _collection = in.readUTF();
        _mongo = _getMongo();
    }

    public void write(DataOutput out)
        throws IOException {
        out.writeUTF( _db );
        out.writeUTF( _collection );
    }    

    public DBCollection collection(){
        return _mongo.getDB( _db ).getCollection( _collection );
    }

    final String _db;
    final String _collection;

    final Mongo _mongo;

    static Mongo _getMongo(){
        if ( _staticMongo == null ){
            try {
                _staticMongo = new Mongo();
            }
            catch ( Exception e ){
                throw new RuntimeException( "can't make new mongo" , e );
            }
        }
        return _staticMongo;
    }

    static Mongo _staticMongo;
}
