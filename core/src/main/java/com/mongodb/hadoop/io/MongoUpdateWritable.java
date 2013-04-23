// BSONWritable.java
/*
 * Copyright 2010 - 2011 10gen Inc.
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

package com.mongodb.hadoop.io;

import com.mongodb.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import org.bson.*;
import org.bson.io.*;
import org.bson.io.Bits;

import java.io.*;
import java.util.*;

/**
 * This is <em>not</em> reusable.
 */

@SuppressWarnings( "deprecation" )
public class MongoUpdateWritable implements Writable {

    private BasicBSONObject query;
    private BasicBSONObject modifiers;
    private boolean upsert;
    private boolean multiUpdate;

    private BSONEncoder enc = new BasicBSONEncoder();
    private BasicOutputBuffer buf = new BasicOutputBuffer();

    public MongoUpdateWritable(BasicBSONObject query, BasicBSONObject modifiers, boolean upsert, boolean multiUpdate){
        this.query = query;
        this.modifiers = modifiers;
        this.upsert = upsert;
        this.multiUpdate = multiUpdate;
    }

    public MongoUpdateWritable(BasicBSONObject query, BasicBSONObject modifiers){
        this(query, modifiers, true, false);
    }
    public BasicBSONObject getQuery(){
        return this.query;
    }

    public BasicBSONObject getModifiers(){
        return this.modifiers;
    }

    public boolean isUpsert(){
        return this.upsert;
    }

    public boolean isMultiUpdate(){
        return this.multiUpdate;
    }

    /**

    /**
     * {@inheritDoc}
     *
     * @see Writable#write(DataOutput)
     */
    public void write( DataOutput out ) throws IOException{
        enc.set( buf );
        enc.putObject( this.query );
        enc.done();
        buf.pipe( out );
        enc.set( buf );
        enc.putObject( this.modifiers );
        enc.done();
        out.writeBoolean(this.upsert);
        out.writeBoolean(this.multiUpdate);
    }


    /**
     * {@inheritDoc}
     *
     * @see Writable#readFields(DataInput)
     */
    public void readFields( DataInput in ) throws IOException{
        BSONDecoder dec = new BasicBSONDecoder();
        BSONCallback cb = new BasicBSONCallback();
        // Read the BSON length from the start of the record
        byte[] l = new byte[4];
        try {
            in.readFully( l );
            int dataLen = Bits.readInt( l );
            byte[] data = new byte[dataLen + 4];
            System.arraycopy( l, 0, data, 0, 4 );
            in.readFully( data, 4, dataLen - 4 );
            dec.decode( data, cb );
            this.query = (BasicBSONObject) cb.get();
            in.readFully( l );
            dataLen = Bits.readInt( l );
            data = new byte[dataLen + 4]; 
            System.arraycopy( l, 0, data, 0, 4 );
            in.readFully( data, 4, dataLen - 4 );
            dec.decode( data, cb );
            this.modifiers = (BasicBSONObject) cb.get();
            this.upsert = in.readBoolean();
            this.multiUpdate = in.readBoolean();
        }
        catch ( Exception e ) {
            /* If we can't read another length it's not an error, just return quietly. */
            // TODO - Figure out how to gracefully mark this as an empty
            log.info( "No Length Header available." + e );
            this.query = new BasicDBObject();
            this.modifiers = new BasicDBObject();
        }

    }

    @Override
    public boolean equals( Object obj ){
        if ( obj == null || getClass() != obj.getClass() )
            return false;
        final MongoUpdateWritable other = (MongoUpdateWritable) obj;
        if(this.upsert != other.upsert || this.multiUpdate != other.multiUpdate){
            return false;
        }
        if( !( this.query != other.query && ( this.query == null || !this.query.equals( other.query ) ) )){
            return true;
        }
        return !( this.modifiers != other.modifiers && ( this.modifiers == null || !this.modifiers.equals( other.modifiers ) ) );
    }

    @Override
    public int hashCode(){
        int hashCode = this.query.hashCode();
        hashCode ^= this.modifiers.hashCode();
        hashCode ^= (this.upsert ? 1 : 0) << 1;
        hashCode ^= (this.multiUpdate ? 1 : 0) << 2;
        return hashCode;
    }

    protected BSONObject _doc;


    private static final Log log = LogFactory.getLog( MongoUpdateWritable.class );

}
