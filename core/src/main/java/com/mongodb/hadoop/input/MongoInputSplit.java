// MongoInputSplit.java
/*
 * Copyright 2010 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.input;

import com.mongodb.*;
import com.mongodb.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;

import java.io.*;
import java.util.*;

public class MongoInputSplit extends InputSplit implements Writable {

    public MongoInputSplit( MongoURI inputURI,
                            String keyField,
                            DBObject query,
                            DBObject fields,
                            DBObject sort,
                            int limit,
                            int skip,
                            boolean noTimeout ){
        log.debug( "Creating a new MongoInputSplit for MongoURI '"
                   + inputURI + "', keyField: " + keyField + ", query: '" + query + "', fieldSpec: '" + fields
                   + "', sort: '" + sort + "', limit: " + limit + ", skip: " + skip + " noTimeout? " + noTimeout + "." );

        _mongoURI = inputURI;
        _keyField = keyField;
        _querySpec = query;
        _fieldSpec = fields;
        _sortSpec = sort;
        _limit = limit;
        _skip = skip;
        _notimeout = noTimeout;
        getCursor();
        getBSONDecoder();
        getBSONEncoder();
    }



    /**
     * This is supposed to return the size of the split in bytes, but for now, for sanity sake we return the # of docs
     * in the split instead.
     *
     * @return
     */
    @Override
    public long getLength(){
        return Long.MAX_VALUE;
    }

    @Override
    public String[] getLocations(){
        return _mongoURI.getHosts().toArray( new String[_mongoURI.getHosts().size()] );
    }

    /**
     * Serialize the Split instance
     */
    public void write( final DataOutput out ) throws IOException{
        BSONEncoder enc = getBSONEncoder();

        BSONObject spec = BasicDBObjectBuilder.start().
                                               add( "uri", _mongoURI.toString() ).
                                               add( "key", _keyField ).
                                               add( "query", _querySpec ).
                                               add( "field", _fieldSpec ).                                              
                                               add( "sort", _sortSpec ).
                                               add( "limit", _limit ).
                                               add( "skip", _skip ).
                                               add( "notimeout", _notimeout ).get();

        byte[] buf = enc.encode( spec );

        out.write( buf );
    }

    public void readFields( DataInput in ) throws IOException{
        BSONDecoder dec = getBSONDecoder();
        BSONCallback cb = new BasicBSONCallback();
        BSONObject spec;
        // Read the BSON length from the start of the record
        byte[] l = new byte[4];
        try {
            in.readFully( l );
            int dataLen = org.bson.io.Bits.readInt( l );
            if ( log.isDebugEnabled() ) log.debug( "*** Expected DataLen: " + dataLen );
            byte[] data = new byte[dataLen + 4];
            System.arraycopy( l, 0, data, 0, 4 );
            in.readFully( data, 4, dataLen - 4 );
            dec.decode( data, cb );
            spec = (BSONObject) cb.get();
            if ( log.isTraceEnabled() ) log.trace( "Decoded a BSON Object: " + spec );
        }
        catch ( Exception e ) {
            /* If we can't read another length it's not an error, just return quietly. */
            // TODO - Figure out how to gracefully mark this as an empty
            log.info( "No Length Header available." + e );
            spec = new BasicDBObject();
        }         
        
        _mongoURI = new MongoURI((String) spec.get( "uri" ));
        _keyField = (String) spec.get( "key" );
        _querySpec = (DBObject) spec.get( "query" );
        _fieldSpec = (DBObject) spec.get( "field" );
        _sortSpec = (DBObject) spec.get( "sort" );
        _limit = (Integer) spec.get( "limit" );
        _skip = (Integer) spec.get( "skip" );
        _notimeout = (Boolean) spec.get( "notimeout" );
        getCursor();

        log.info( "Deserialized MongoInputSplit ... { length = " + getLength() + ", locations = "
                   + Arrays.toString( getLocations() ) + ", keyField = " + _keyField + ", query = " + _querySpec
                   + ", fields = " + _fieldSpec + ", sort = " + _sortSpec + ", limit = " + _limit + ", skip = "
                   + _skip + ", noTimeout = " + _notimeout + "}" );
    }

    DBCursor getCursor(){
        // Return the cursor with the split's query, etc. already slotted in for
        // them.
        // todo - support limit/skip
        if ( _cursor == null ){
            _cursor = MongoConfigUtil.getCollection( _mongoURI ).find( _querySpec, _fieldSpec ).sort( _sortSpec );
            if (_notimeout) _cursor.setOptions( Bytes.QUERYOPTION_NOTIMEOUT );
            _cursor.slaveOk();
        }

        return _cursor;
    }

    BSONEncoder getBSONEncoder(){
        if (_bsonEncoder == null) 
            _bsonEncoder = new BasicBSONEncoder();
        return _bsonEncoder;
    }
    
    BSONDecoder getBSONDecoder(){
        if (_bsonDecoder == null)
            _bsonDecoder = new BasicBSONDecoder();
        return _bsonDecoder;
    }

    @Override
    public String toString(){
        return "MongoInputSplit{URI=" + _mongoURI + ", keyField=" + _keyField + ", query=" + _querySpec + ", sort=" + _sortSpec + ", fields=" + _fieldSpec + '}';
    }

    public MongoInputSplit(){ }

    public MongoURI getMongoURI(){
        return _mongoURI;
    }

    public DBObject getQuerySpec(){
        return _querySpec;
    }

    public DBObject getFieldSpec(){
        return _fieldSpec;
    }

    public DBObject getSortSpec(){
        return _sortSpec;
    }

    public int getLimit(){
        return _limit;
    }

    public int getSkip(){
        return _skip;
    }

    /**
     * The field to use as the Mapper Key
     */
    public String getKeyField(){
        return _keyField;
    }

    private MongoURI _mongoURI;
    private String _keyField;
    private DBObject _querySpec;
    private DBObject _fieldSpec;
    private DBObject _sortSpec;
    private boolean _notimeout;
    private int _limit = 0;
    private int _skip = 0;
    private long _length = -1;
    private transient DBCursor _cursor;
    private transient BSONEncoder _bsonEncoder;
    private transient BSONDecoder _bsonDecoder;

    private static final Log log = LogFactory.getLog( MongoInputSplit.class );

}
