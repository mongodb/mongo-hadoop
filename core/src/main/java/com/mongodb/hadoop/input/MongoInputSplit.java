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
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;

import java.io.*;
import java.util.*;

public class MongoInputSplit extends InputSplit implements Writable {

    public MongoInputSplit( MongoURI inputURI,
                            DBObject query,
                            DBObject fields,
                            DBObject sort,
                            int limit,
                            int skip ){
        LOG.info( "Creating a new MongoInputSplit for MongoURI '"
                   + inputURI + "', query: '" + query + "', fieldSpec: '" + fields + "', sort: '"
                   + sort + "', limit: " + limit + ", skip: " + skip + " ." );

        _mongoURI = inputURI;
        _querySpec = query;
        _fieldSpec = fields;
        _sortSpec = sort;
        _limit = limit;
        _skip = skip;
        getCursor();
        
        getBsonDecoder();
        getBsonEncoder();
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
        byte[] buf;
        getBsonEncoder();
        
        out.writeUTF( _mongoURI.toString() );
        
        buf = _bsonEncoder.encode(_querySpec);
        out.writeInt(buf.length);
        out.write(buf);
        
        buf = _bsonEncoder.encode(_fieldSpec);
        out.writeInt(buf.length);
        out.write(buf);
        
        buf = _bsonEncoder.encode(_sortSpec);
        out.writeInt(buf.length);
        out.write(buf);
        
        out.writeInt( _limit );
        out.writeInt( _skip );
    }

    public void readFields( DataInput in ) throws IOException{
    	
        _mongoURI = new MongoURI( in.readUTF() );
        
        byte[] buf;
        getBsonDecoder();
        
        buf = new byte[in.readInt()];
        in.readFully(buf, 0, buf.length);
        _querySpec = new BasicDBObject(_bsonDecoder.readObject(buf).toMap());
        
        buf = new byte[in.readInt()];
        in.readFully(buf, 0, buf.length);
        _fieldSpec = new BasicDBObject(_bsonDecoder.readObject(buf).toMap());
        
        buf = new byte[in.readInt()];
        in.readFully(buf, 0, buf.length);
        _sortSpec = new BasicDBObject(_bsonDecoder.readObject(buf).toMap());
        
        _limit = in.readInt();
        _skip = in.readInt();
        getCursor();

        LOG.info( "Deserialized MongoInputSplit ... { length = " + getLength() + ", locations = "
                   + Arrays.toString( getLocations() ) + ", query = " + _querySpec
                   + ", fields = " + _fieldSpec + ", sort = " + _sortSpec + ", limit = " + _limit + ", skip = "
                   + _skip + "}" );
    }

    DBCursor getCursor(){
        // Return the cursor with the split's query, etc. already slotted in for
        // them.
        // todo - support limit/skip
        if ( _cursor == null ){
            _cursor = MongoConfigUtil.getCollection( _mongoURI ).find( _querySpec, _fieldSpec ).sort( _sortSpec );
            // It would be exceptionally bad for cursors to time out reading data for Hadoop. Disable
            _cursor.setOptions( Bytes.QUERYOPTION_NOTIMEOUT );
            _cursor.slaveOk();
        }

        return _cursor;
    }

    BSONEncoder getBsonEncoder(){
    	if (_bsonEncoder == null){
    		_bsonEncoder = new BSONEncoder();
    	}
    	return _bsonEncoder;
    }
    
    BSONDecoder getBsonDecoder(){
    	if (_bsonDecoder == null){
    		_bsonDecoder = new BSONDecoder();
    	}
    	return _bsonDecoder;
    }

    @Override
    public String toString(){
        return "MongoInputSplit{URI=" + _mongoURI + ", query=" + _querySpec + ", sort=" + _sortSpec + ", fields=" + _fieldSpec + '}';
    }

    public MongoInputSplit(){ }

    private MongoURI _mongoURI;

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

    private DBObject _querySpec;
    private DBObject _fieldSpec;
    private DBObject _sortSpec;
    private int _limit = 0;
    private int _skip = 0;
    private long _length = -1;
    private transient DBCursor _cursor;
    
    private transient BSONEncoder _bsonEncoder;
    private transient BSONDecoder _bsonDecoder;

    private static final Log LOG = LogFactory.getLog( MongoInputSplit.class );

}
