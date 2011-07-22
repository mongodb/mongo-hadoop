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

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import org.bson.*;
import org.bson.io.*;

/** This is <em>not</em> reusable. */

@SuppressWarnings( "deprecation" )
public class BSONWritable implements BSONObject, WritableComparable {

    /**
     * Constructs a new instance.
     */
    public BSONWritable() {
        _doc = new BasicBSONObject();
    }

    /**
     * Copy constructor, copies data from an existing BSONWritable
     * 
     * @param other
     *            The BSONWritable to copy from
     */
    public BSONWritable(BSONWritable other) {
        this();
        copy( other );
    }

    /**
     * Constructs a new instance around an existing BSONObject
     */
    public BSONWritable(BSONObject doc) {
        this();
        putAll( doc );
    }

    /**
     * {@inheritDoc}
     * 
     * @see BSONObject#put(String,Object)
     */
    public Object put( String key , Object value ){
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
        BSONEncoder enc = new BSONEncoder();
        BasicOutputBuffer buf = new BasicOutputBuffer();
        enc.set( buf );
        enc.putObject( _doc );
        enc.done();
        final int datalen = buf.size();
        if (datalen <= 0){
            byte[] ba = buf.toByteArray();
            log.error("write(): datalen is: "+datalen+" byte[] len is "+ba.length);
            out.writeInt(ba.length);
            out.write(ba);
        } else {
            out.writeInt(datalen);
            buf.pipe(out);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see Writable#readFields(DataInput)
     */
    public void readFields( DataInput in ) throws IOException{
        BSONDecoder dec = new BSONDecoder();
        BSONCallback cb = new BasicBSONCallback();
        // Read the BSON length from the start of the record
        int dataLen = in.readInt();
        if (dataLen <= 0)
            log.error("readFields(): datalen is: "+dataLen);
        byte[] buf = new byte[dataLen];
        in.readFully( buf );
        dec.decode( buf, cb );
        _doc = (BSONObject) cb.get();
        log.debug( "Decoded a BSON Object: " + _doc );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString(){
        BSONEncoder enc = new BSONEncoder();
        BasicOutputBuffer buf = new BasicOutputBuffer();
        enc.set( buf );
        enc.putObject( _doc );
        enc.done();
        String str = buf.asString();
        log.debug( "Output As String: '" + str + "'" );
        return str;
    }

    /** Used by child copy constructors. */
    protected synchronized void copy( Writable other ){
        if ( other != null ) {
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
        else {
            throw new IllegalArgumentException( "source map cannot be null" );
        }
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super( BSONWritable.class );
        }

        public int compare( WritableComparable a , WritableComparable b ){

            if ( a instanceof BSONWritable && b instanceof BSONWritable ) {
                return ( (BSONWritable) a )._doc.toString().compareTo( ( (BSONWritable) b )._doc.toString() );
            }
            else {
                return -1;
            }
        }

    }

    static { // register this comparator
        WritableComparator.define( BSONWritable.class, new Comparator() );
    }

    @Override
    public int compareTo( Object o ){
        return new Comparator().compare( this, o );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;
        final BSONWritable other = (BSONWritable) obj;
        if (this._doc != other._doc && (this._doc == null || !this._doc.equals(other._doc)))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return (this._doc != null ? this._doc.hashCode() : 0);
    }

    protected BSONObject _doc;

    private static final Log log = LogFactory.getLog( BSONWritable.class );

}
