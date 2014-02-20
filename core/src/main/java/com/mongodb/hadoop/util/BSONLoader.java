/*
 * Copyright 2010-2013 10gen Inc.
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

package com.mongodb.hadoop.util;

import com.mongodb.*;
import org.apache.commons.logging.*;
import org.bson.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class BSONLoader implements Iterable<BSONObject>, Iterator<BSONObject> {
    
    public BSONLoader(final InputStream input) {
        _input = new DataInputStream( input );
    }

    public Iterator<BSONObject> iterator(){
        return this;
    }

    public boolean hasNext(){
        checkHeader();
        return hasMore.get();
    }
    
    private synchronized void checkHeader(){
        // Read the BSON length from the start of the record
        byte[] l = new byte[4];
        try {
            _input.readFully( l );
            nextLen = org.bson.io.Bits.readInt( l );
            nextHdr = l;
            hasMore.set( true );
        } catch (Exception e) {
            log.debug( "Failed to get next header: " + e, e );
            hasMore.set( false );
            try {
                _input.close();
            }
            catch ( IOException e1 ) { }
        }
    }

    public BSONObject next(){
        try {
            byte[] data = new byte[nextLen + 4];
            System.arraycopy( nextHdr, 0, data, 0, 4 );
            _input.readFully( data, 4, nextLen - 4 );
            decoder.decode( data, callback );
            return (BSONObject) callback.get();
        }
        catch ( IOException e ) {
            /* If we can't read another length it's not an error, just return quietly. */
            log.info( "No Length Header available." + e );
            hasMore.set( false );
            try {
                _input.close();
            }
            catch ( IOException e1 ) { }
            throw new NoSuchElementException("Iteration completed.");
        }
    }

    public void remove(){
        throw new UnsupportedOperationException();
    }

    private final BSONDecoder decoder = new BasicBSONDecoder();
    private final BSONCallback callback = new BasicBSONCallback();

    private volatile byte[] nextHdr;
    private volatile int nextLen;
    private AtomicBoolean hasMore = new AtomicBoolean( true );
    private final DataInputStream _input;
    private static final Log log = LogFactory.getLog( BSONLoader.class );
}
