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
import com.mongodb.hadoop.util.MongoIDRange;

import com.mongodb.util.JSON;

public class MongoInputSplit extends InputSplit implements Writable {
    private static final Log log = LogFactory.getLog(MongoInputSplit.class);

    private MongoIDRange idRange;
    private ArrayList<MongoURI> mongoURIs = new ArrayList<MongoURI>(1);
    private DBObject querySpec;
    private DBObject fieldSpec;
    private DBObject sortSpec;
    private int limit = 0;
    private int skip = 0;

    public MongoInputSplit(MongoIDRange mongoIDs, MongoURI inputURI, DBObject query, DBObject fields, DBObject sort, int limit, int skip) {
        log.debug("Creating a new MongoInputSplit for MongoURI '" + inputURI + "', query: '" + query + "', fieldSpec: '" + fields + "', sort: '" + sort + "', limit: " + limit  + ", skip: " + skip + " against " + mongoIDs.size() + " IDs.");
        this.idRange = mongoIDs;
        this.mongoURIs.add(inputURI);
        this.querySpec = query;
        this.fieldSpec = fields;
        this.sortSpec = sort;
        this.limit = limit;
        this.skip = skip;
    }

    public MongoInputSplit(MongoIDRange mongoIDs, ArrayList<MongoURI> inputURIs, DBObject query, DBObject fields, DBObject sort, int limit, int skip) {
        log.debug("Creating a new MongoInputSplit for MongoURI '" + inputURIs + "', fieldSpec: '" + fields + "' against " + mongoIDs.size() + " IDs.");
        this.idRange = mongoIDs;
        this.fieldSpec = fields;
        this.mongoURIs = inputURIs; // todo - should we clone / copy?
        this.querySpec = query;
        this.fieldSpec = fields;
        this.sortSpec = sort;
        this.limit = limit;
        this.skip = skip;
    }

    public MongoInputSplit() {}

    public long getLength() {
        return idRange.size();
    }
   
    public String[] getLocations() {
        // TODO - right now the URI even w/ multi servers should return one string.
        // What about for replica sets, shards, etc? 
        String[] locs = new String[mongoURIs.size()];
        for (int i = 0; i < mongoURIs.size(); i++) {
            locs[i] = mongoURIs.get(i).toString();
        }
        return locs;
    }
    
    /** 
     * Serialize the Split instance 
     */
    public void write(DataOutput out) throws IOException {
        ObjectOutputStream objOut = new ObjectOutputStream((OutputStream) out);
        objOut.writeObject(idRange);

        // TODO - Use object outputstream instead of going to <-> from string?
        out.writeInt(getLocations().length);
        for (String _uri : getLocations())  {
            out.writeUTF(_uri.toString());
        }

        out.writeUTF(JSON.serialize(querySpec));
        out.writeUTF(JSON.serialize(fieldSpec));
        out.writeUTF(JSON.serialize(sortSpec));
        out.writeInt(limit);
        out.writeInt(skip);
        objOut.close();
    }

    public void readFields(DataInput in) throws IOException {
        ObjectInputStream objIn = new ObjectInputStream((InputStream) in);
        try {
            idRange = (MongoIDRange) objIn.readObject();
            log.trace("ID Range: " + idRange);
        } 
        catch (ClassNotFoundException e) {
            throw new IOException("Cannot deserialize IDRange.", e);
        }


        int numURIs = in.readInt();
        log.trace("Expecting to read in " + numURIs + " MongoURIs.");
        for (int x = 0; x < numURIs; x++) {
            mongoURIs.add(new MongoURI(in.readUTF()));
        }
        log.trace("Mongo URIs: " + mongoURIs);

        querySpec = (DBObject) JSON.parse(in.readUTF());
        fieldSpec = (DBObject) JSON.parse(in.readUTF());
        sortSpec = (DBObject) JSON.parse(in.readUTF());
        limit = in.readInt();
        skip = in.readInt();

        
        log.info("Deserialized MongoInputSplit ... { ids = " + idRange + ", length = " + getLength() + ", locations = " + getLocations() + ", query = " + querySpec + ", fields = " + fieldSpec + ", sort = " + sortSpec + ", limit = " + limit + ", skip = " + skip + "}");


        objIn.close();
    }
    
    DBCursor getCursor() {
        // Hardcoded to take first url - TODO - Fix me
        MongoURI _addr = mongoURIs.get(0);
        // Return the cursor with the split's query, etc. already slotted in for them.
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start("$query", querySpec);
        b.add("$min", new BasicDBObject("_id", idRange.getMin()));
        b.add("$max", new BasicDBObject("_id", idRange.getMax()));
        // todo - support limit/skip
        DBObject q = b.get();
        log.info("Addr: " + _addr + " Q: " + q);
        DBCursor cursor = MongoConfigUtil.getCollection(_addr).find(q, fieldSpec).sort(sortSpec);
        return cursor;
    }


}


