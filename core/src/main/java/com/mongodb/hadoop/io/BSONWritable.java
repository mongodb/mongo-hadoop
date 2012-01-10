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
public class BSONWritable implements BSONObject, WritableComparable {

    /**
     * Constructs a new instance.
     */
    public BSONWritable(){
        _doc = new BasicBSONObject();
    }

    /**
     * Copy constructor, copies data from an existing BSONWritable
     *
     * @param other The BSONWritable to copy from
     */
    public BSONWritable( BSONWritable other ){
        this();
        copy( other );
    }

    /**
     * Constructs a new instance around an existing BSONObject
     *
     * @param doc
     */
    public BSONWritable( BSONObject doc ){
        this();
        putAll( doc );
    }

    /**
     * {@inheritDoc}
     *
     * @see BSONObject#put(String , Object)
     */
    public Object put( String key, Object value ){
        return _doc.put( key, value );
    }

    /**
     * {@inheritDoc}
     *
     * @see BSONObject#putAll(BSONObject)
     */
    public void putAll( BSONObject otherDoc ){
        _doc.putAll( otherDoc );
    }

    /**
     * {@inheritDoc}
     *
     * @see BSONObject#putAll(Map)
     */
    public void putAll( Map otherMap ){
        _doc.putAll( otherMap );
    }

    /**
     * {@inheritDoc}
     *
     * @see BSONObject#get(String)
     */
    public Object get( String key ){
        return _doc.get( key );
    }

    /**
     * {@inheritDoc}
     *
     * @see BSONObject#toMap()
     */
    public Map toMap(){
        return _doc.toMap();
    }

    /**
     * {@inheritDoc}
     *
     * @see BSONObject#removeField(String)
     */
    public Object removeField( String key ){
        return _doc.removeField( key );
    }

    /**
     * {@inheritDoc}
     *
     * @see BSONObject#containsKey(String)
     */
    public boolean containsKey( String key ){
        return _doc.containsKey( key );
    }

    /**
     * {@inheritDoc}
     *
     * @see BSONObject#containsField(String)
     */
    public boolean containsField( String fieldName ){
        return _doc.containsField( fieldName );
    }

    /**
     * {@inheritDoc}
     *
     * @see BSONObject#keySet()
     */
    public Set<java.lang.String> keySet(){
        return _doc.keySet();
    }

    /**
     * {@inheritDoc}
     *
     * @see Writable#write(DataOutput)
     */
    public void write( DataOutput out ) throws IOException{

        BSONEncoder enc = new BasicBSONEncoder();
        BasicOutputBuffer buf = new BasicOutputBuffer();
        enc.set( buf );
        enc.putObject( _doc );
        enc.done();
        buf.pipe( out );
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
            if ( log.isDebugEnabled() ) log.debug( "*** Expected DataLen: " + dataLen );
            byte[] data = new byte[dataLen + 4];
            System.arraycopy( l, 0, data, 0, 4 );
            in.readFully( data, 4, dataLen - 4 );
            dec.decode( data, cb );
            _doc = (BSONObject) cb.get();
            if ( log.isTraceEnabled() ) log.trace( "Decoded a BSON Object: " + _doc );
        }
        catch ( Exception e ) {
            /* If we can't read another length it's not an error, just return quietly. */
            // TODO - Figure out how to gracefully mark this as an empty
            log.info( "No Length Header available." + e );
            _doc = new BasicDBObject();
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString(){
        BSONEncoder enc = new BasicBSONEncoder();
        BasicOutputBuffer buf = new BasicOutputBuffer();
        enc.set( buf );
        enc.putObject( _doc );
        enc.done();
        String str = buf.asString();
        log.debug( "Output As String: '" + str + "'" );
        return str;
    }

    /**
     * Used by child copy constructors.
     *
     * @param other
     */
    protected synchronized void copy( Writable other ){
        if ( other != null ){
            try {
                DataOutputBuffer out = new DataOutputBuffer();
                other.write( out );
                DataInputBuffer in = new DataInputBuffer();
                in.reset( out.getData(), out.getLength() );
                readFields( in );

            }
            catch ( IOException e ) {
                throw new IllegalArgumentException( "map cannot be copied: " + e.getMessage() );
            }

        }
        else{
            throw new IllegalArgumentException( "source map cannot be null" );
        }
    }


    static{ // register this comparator
        WritableComparator.define( BSONWritable.class, new BSONComparator() );
    }

    public int compareTo( Object o ){
        if ( log.isTraceEnabled() ) log.trace( " ************ Compare: '" + this + "' to '" + o + "'" );
        return new BSONComparator().compare( this, o );
    }

    private static final byte[] HEX_CHAR = new byte[] {
            '0' , '1' , '2' , '3' , '4' , '5' , '6' , '7' , '8' ,
            '9' , 'A' , 'B' , 'C' , 'D' , 'E' , 'F'
    };

    protected static void dumpBytes( BasicOutputBuffer buf ){// Temp debug output
        dumpBytes( buf.toByteArray() );
    }

    protected static void dumpBytes( byte[] buffer ){
        StringBuilder sb = new StringBuilder( 2 + ( 3 * buffer.length ) );

        for ( byte b : buffer ){
            sb.append( "0x" ).append( (char) ( HEX_CHAR[( b & 0x00F0 ) >> 4] ) ).append(
                    (char) ( HEX_CHAR[b & 0x000F] ) ).append( " " );
        }

        log.info( "Byte Dump: " + sb.toString() );
    }

    @Override
    public boolean equals( Object obj ){
        if ( obj == null || getClass() != obj.getClass() )
            return false;
        final BSONWritable other = (BSONWritable) obj;
        return !( this._doc != other._doc && ( this._doc == null || !this._doc.equals( other._doc ) ) );
    }

    @Override
    public int hashCode(){
        return ( this._doc != null ? this._doc.hashCode() : 0 );
    }

    protected BSONObject _doc;

    private static final Log log = LogFactory.getLog( BSONWritable.class );


}
