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
import com.mongodb.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.util.*;

public class MongoInputSplit extends InputSplit implements Writable {

    public MongoInputSplit( MongoURI inputURI,
                            DBObject query,
                            DBObject fields,
                            DBObject sort,
                            int limit,
                            int skip ){
        if ( LOG.isDebugEnabled() ){
            LOG.debug( "Creating a new MongoInputSplit for MongoURI '"
                       + inputURI + "', query: '" + query + "', fieldSpec: '" + fields + "', sort: '"
                       + sort + "', limit: " + limit + ", skip: " + skip + " ." );
        }

        _mongoURI = inputURI;
        _querySpec = query;
        _fieldSpec = fields;
        _sortSpec = sort;
        _limit = limit;
        _skip = skip;
        getCursor();
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
    @Override
    public void write( final DataOutput out ) throws IOException{
        out.writeUTF( _mongoURI.toString() );
        out.writeUTF( JSON.serialize( _querySpec ) );
        out.writeUTF( JSON.serialize( _fieldSpec ) );
        out.writeUTF( JSON.serialize( _sortSpec ) );
        out.writeInt( _limit );
        out.writeInt( _skip );
    }

    @Override
    public void readFields( DataInput in ) throws IOException{
        _mongoURI = new MongoURI( in.readUTF() );
        _querySpec = (DBObject) JSON.parse( in.readUTF() );
        _fieldSpec = (DBObject) JSON.parse( in.readUTF() );
        _sortSpec = (DBObject) JSON.parse( in.readUTF() );
        _limit = in.readInt();
        _skip = in.readInt();
        getCursor();

        if ( LOG.isDebugEnabled() ){
            LOG.debug( "Deserialized MongoInputSplit ... { length = " + getLength() + ", locations = "
                       + Arrays.toString( getLocations() ) + ", query = " + _querySpec
                       + ", fields = " + _fieldSpec + ", sort = " + _sortSpec + ", limit = " + _limit + ", skip = "
                       + _skip + "}" );
        }
    }

    DBCursor getCursor(){
        // Return the cursor with the split's query, etc. already slotted in for
        // them.
        // todo - support limit/skip
        if ( _cursor == null ){
            _cursor = MongoConfigUtil.getCollection( _mongoURI ).find( _querySpec, _fieldSpec ).sort( _sortSpec );
            _cursor.slaveOk();
        }

        return _cursor;
    }


    @Override
    public String toString(){
        return "MongoInputSplit{URI=" + _mongoURI + ", query=" + _querySpec + '}';
    }

    public MongoInputSplit(){ }

    private MongoURI _mongoURI;
    private DBObject _querySpec;
    private DBObject _fieldSpec;
    private DBObject _sortSpec;
    private int _limit = 0;
    private int _skip = 0;
    private long _length = -1;
    private transient DBCursor _cursor;

    private static final Log LOG = LogFactory.getLog( MongoInputSplit.class );

}
