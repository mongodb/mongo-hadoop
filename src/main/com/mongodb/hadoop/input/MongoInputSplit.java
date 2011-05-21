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

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.*;
import com.mongodb.hadoop.util.*;
import com.mongodb.util.*;

public class MongoInputSplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {
    private static final Log log = LogFactory.getLog( MongoInputSplit.class );

    public MongoInputSplit(MongoURI inputURI , DBObject query , DBObject fields , DBObject sort , int limit , int skip) {
        log.debug( "Creating a new MongoInputSplit for MongoURI '" + inputURI + "', query: '" + query + "', fieldSpec: '" + fields + "', sort: '"
                + sort + "', limit: " + limit + ", skip: " + skip + " ." );
        _mongoURI = inputURI;
        _querySpec = query;
        _fieldSpec = fields;
        _sortSpec = sort;
        _limit = limit;
        _skip = skip;
    }

    public MongoInputSplit() {
    }
    /** This is supposed to return the size of the split in bytes, but is hardcoded
     to return a constant value*/
    public long getLength() {
        //This takes a really long time.  Hadoop calls this when it is deciding
        //how to divvy up the splits.  As a result the whole database is sequenced
        //through before the real number crunching begins.
        //return getCursor().size();
        return 200000000; //200 million is the default mongo chunk size
    }

    public String[] getLocations(){
        final List<String> hosts = _mongoURI.getHosts();
        return hosts.toArray( new String[hosts.size()] );
    }

    /**
     * Serialize the Split instance
     */

    public void write( DataOutput out ) throws IOException{
        final ObjectOutputStream objOut = new ObjectOutputStream( (OutputStream) out );
        // TODO - Use object outputstream instead of going to <-> from string?
        out.writeUTF( _mongoURI.toString() );

        out.writeUTF( JSON.serialize( _querySpec ) );
        out.writeUTF( JSON.serialize( _fieldSpec ) );
        out.writeUTF( JSON.serialize( _sortSpec ) );
        out.writeInt( _limit );
        out.writeInt( _skip );
        objOut.close();
    }

    public void readFields( DataInput in ) throws IOException{
        final ObjectInputStream objIn = new ObjectInputStream( (InputStream) in );

        _mongoURI = new MongoURI( in.readUTF() );
        _querySpec = (DBObject) JSON.parse( in.readUTF() );
        _fieldSpec = (DBObject) JSON.parse( in.readUTF() );
        _sortSpec = (DBObject) JSON.parse( in.readUTF() );
        _limit = in.readInt();
        _skip = in.readInt();

        log.info( "Deserialized MongoInputSplit ... { length = " + getLength() + ", locations = " + Arrays.toString(getLocations()) + ", query = " + _querySpec
                + ", fields = " + _fieldSpec + ", sort = " + _sortSpec + ", limit = " + _limit + ", skip = " + _skip + "}" );

        objIn.close();
    }

    DBCursor getCursor(){
        // Return the cursor with the split's query, etc. already slotted in for
        // them.
        // todo - support limit/skip
        final DBCursor cursor = MongoConfigUtil.getCollection( _mongoURI ).find( _querySpec, _fieldSpec ).sort( _sortSpec );
        cursor.slaveOk();
        return cursor;
    }

    private MongoURI _mongoURI;
    private DBObject _querySpec;
    private DBObject _fieldSpec;
    private DBObject _sortSpec;
    private int _limit = 0;
    private int _skip = 0;

}
