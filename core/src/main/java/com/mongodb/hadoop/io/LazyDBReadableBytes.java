package com.mongodb.hadoop.io;
import org.bson.io.BSONByteBuffer;
import org.bson.LazyBSONCallback; 
import com.mongodb.LazyWriteableDBObject;

public class LazyDBReadableBytes extends LazyWriteableDBObject{

	public LazyDBReadableBytes(BSONByteBuffer buff, int offset, LazyDBReadableBytesCallback cbk){
		super(buff, offset, cbk);
	}
    public LazyDBReadableBytes(BSONByteBuffer buff, LazyDBReadableBytesCallback cbk){
		super(buff, cbk);
	}
    public LazyDBReadableBytes(byte[] data, int offset, LazyDBReadableBytesCallback cbk){
		super(data, offset, cbk);
	}
	public LazyDBReadableBytes(byte[] data, LazyDBReadableBytesCallback cbk){
		super(data, cbk);
	}

	public LazyDBReadableBytes(byte[] data){
		super(data, new LazyDBReadableBytesCallback(null));
	}

    public BSONByteBuffer getRawData(){
        return this._input;
    }

}
