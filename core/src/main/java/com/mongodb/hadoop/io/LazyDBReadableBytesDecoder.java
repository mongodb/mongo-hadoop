package com.mongodb.hadoop.io;
import com.mongodb.*;
import org.bson.*;
import java.io.InputStream;
import java.io.IOException;

public class LazyDBReadableBytesDecoder extends LazyWriteableDBDecoder implements DBDecoder{

    static class LazyDBReadableBytesDecoderFactory implements DBDecoderFactory {
        @Override                                                                                                                                                                                                                      
        public DBDecoder create( ){                                                                                                                                                                                                    
            return new LazyDBReadableBytesDecoder();
        }                                                                                                                                                                                                                              
    }                                                                                                                                                                                                                                  

    public static DBDecoderFactory FACTORY = new LazyDBReadableBytesDecoderFactory();

    public DBCallback getDBCallback(DBCollection collection) {                                                                                                                                                                         
        return new LazyDBReadableBytesCallback(collection);
    }                                                                                                                                                                                                                                  

	@Override
    public BSONObject readObject(InputStream in) throws IOException {
        BSONCallback c = new LazyDBReadableBytesCallback(null);
        decode( in , c );                                                                                                                                                                                                              
        return (BSONObject)c.get();
    }


}

