// BSONWritable.java
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

package com.mongodb.hadoop.io;

import java.io.*;
import java.util.*;

import org.bson.*;
import org.bson.io.*;

import org.apache.commons.logging.*;

import org.apache.hadoop.io.*;

@SuppressWarnings( "deprecation" )
public class BSONWritable implements BSONObject, Writable {

    private static final Log log = LogFactory.getLog( BSONWritable.class );

    /**
     * Constructs a new instance.
     */
    public BSONWritable() {
        _doc = new BasicBSONObject();
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
        out.write( buf.toByteArray() );
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
        byte[] buf = new byte[dataLen];
        in.readFully( buf );
        dec.decode( buf, cb );
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

    final BSONObject _doc;
}
