// MongoInputSplit.java

package com.mongodb.hadoop.input;

import java.io.*;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.bson.*;

import org.bson.types.ObjectId;
import com.mongodb.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.util.MongoConfigUtil;

import com.mongodb.util.JSON;

public class MongoInputSplit extends InputSplit implements Writable {
    private static final Log log = LogFactory.getLog(MongoInputSplit.class);

    public MongoInputSplit(MongoURI inputURI, DBObject query, DBObject fields, DBObject sort, int limit, int skip) {
        log.debug("Creating a new MongoInputSplit for MongoURI '" + inputURI + "', query: '" + query + "', fieldSpec: '" + fields + "', sort: '" + sort + "', limit: " + limit  + ", skip: " + skip  + " .");
        this._mongoURI = inputURI;
        this._querySpec = query;
        this._fieldSpec = fields;
        this._sortSpec = sort;
        this._limit = limit;
        this._skip = skip;
    }

    public MongoInputSplit() {}

    public long getLength() {
        return getCursor().size();
    }
   
    public String[] getLocations() {
        List<String> hosts = _mongoURI.getHosts();
        return hosts.toArray(new String[hosts.size()]);
    }
    
    /** 
     * Serialize the Split instance 
     */
    public void write(DataOutput out) throws IOException {
        ObjectOutputStream objOut = new ObjectOutputStream((OutputStream) out);
        // TODO - Use object outputstream instead of going to <-> from string?
        out.writeUTF(_mongoURI.toString());

        out.writeUTF(JSON.serialize(_querySpec));
        out.writeUTF(JSON.serialize(_fieldSpec));
        out.writeUTF(JSON.serialize(_sortSpec));
        out.writeInt(_limit);
        out.writeInt(_skip);
        objOut.close();
    }

    public void readFields(DataInput in) throws IOException {
        ObjectInputStream objIn = new ObjectInputStream((InputStream) in);

        _mongoURI = new MongoURI(in.readUTF());
        _querySpec = (DBObject) JSON.parse(in.readUTF());
        _fieldSpec = (DBObject) JSON.parse(in.readUTF());
        _sortSpec = (DBObject) JSON.parse(in.readUTF());
        _limit = in.readInt();
        _skip = in.readInt();

        log.info("Deserialized MongoInputSplit ... { length = " + getLength() + ", locations = " + getLocations() + ", query = " + _querySpec + ", fields = " + _fieldSpec + ", sort = " + _sortSpec + ", limit = " + _limit + ", skip = " + _skip + "}");


        objIn.close();
    }
    
    DBCursor getCursor() {
        // Return the cursor with the split's query, etc. already slotted in for them.
        // todo - support limit/skip
        DBCursor cursor = MongoConfigUtil.getCollection(_mongoURI).find(_querySpec, _fieldSpec).sort(_sortSpec);
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


