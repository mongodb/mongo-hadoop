package com.mongodb.hadoop.io;

import com.mongodb.*;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 *
 */
public class LazyDBReadableBytesCallback extends LazyDBCallback {

    public LazyDBReadableBytesCallback( DBCollection coll ){
        super(coll);
    }

    @Override
    public Object createObject( byte[] data, int offset ){
        LazyDBReadableBytes o = new LazyDBReadableBytes( data, offset, this );
        Iterator it = o.keySet().iterator();
        if ( it.hasNext() && it.next().equals( "$ref" ) &&
             o.containsField( "$id" ) ){
            return new DBRef( null, o );
        }
        return o;
    }

    private static final Logger log = Logger.getLogger( LazyDBReadableBytesCallback.class.getName() );
}

