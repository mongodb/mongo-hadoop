// MongoInputSplit.java

package com.mongodb.hadoop.input;

import java.io.*;
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

    private HashSet<ObjectId> idSet;
    private ArrayList<MongoURI> mongoURIs = new ArrayList<MongoURI>(1);
    private DBObject querySpec;
    private DBObject fieldSpec;
    private DBObject sortSpec;
    private int limit = 0;
    private int skip = 0;

    public MongoInputSplit(HashSet<ObjectId> mongoIDs, MongoURI inputURI, DBObject query, DBObject fields, DBObject sort, int limit, int skip) {
        log.debug("Creating a new MongoInputSplit for MongoURI '" + inputURI + "', query: '" + query + "', fieldSpec: '" + fields + "', sort: '" + sort + "', limit: " + limit  + ", skip: " + skip + " against " + mongoIDs.size() + " IDs.");
        this.idSet = mongoIDs;
        this.mongoURIs.add(inputURI);
        this.querySpec = query;
        this.fieldSpec = fields;
        this.sortSpec = sort;
        this.limit = limit;
        this.skip = skip;
    }

    public MongoInputSplit(HashSet<ObjectId> mongoIDs, ArrayList<MongoURI> inputURIs, DBObject query, DBObject fields, DBObject sort, int limit, int skip) {
        log.debug("Creating a new MongoInputSplit for MongoURI '" + inputURIs + "', fieldSpec: '" + fields + "' against " + mongoIDs.size() + " IDs.");
        this.idSet = mongoIDs;
        /*this.mongoURIs.ensureCapacity(inputURIs.size());*/
        this.fieldSpec = fields;
        log.debug("Creating a new MongoInputSplit for MongoURI '" + inputURIs + "', query: '" + query + "', fieldSpec: '" + fields + "', sort: '" + sort + "', limit: " + limit  + ", skip: " + skip + " against " + mongoIDs.size() + " IDs.");
        this.idSet = mongoIDs;
        this.mongoURIs = inputURIs; // todo - should we clone / copy?
        this.querySpec = query;
        this.fieldSpec = fields;
        this.sortSpec = sort;
        this.limit = limit;
        this.skip = skip;
    }

    public MongoInputSplit() {}

    public long getLength() {
        return idSet.size();
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
        out.writeInt(idSet.size());
        for (ObjectId _id : idSet)  {
            out.writeUTF(_id.toString());
        }

        out.writeInt(getLocations().length);
        for (String _uri : getLocations())  {
            out.writeUTF(_uri.toString());
        }

        out.writeUTF(JSON.serialize(querySpec));
        out.writeUTF(JSON.serialize(fieldSpec));
        out.writeUTF(JSON.serialize(sortSpec));
        out.writeInt(limit);
        out.writeInt(skip);
    }

    public void readFields(DataInput in) throws IOException {
        int numIDs = in.readInt();
        log.trace("Expecting to read in " + numIDs + " ObjectIDs");
        idSet = new HashSet<ObjectId>(numIDs);
        for (int i = 0; i < numIDs; i++)  {
            idSet.add(new ObjectId(in.readUTF()));
        }
        log.trace("ID Set: " + idSet);

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

        
        log.info("Deserialized MongoInputSplit ... { ids = " + idSet + ", length = " + getLength() + ", locations = " + getLocations() + ", query = " + querySpec + ", fields = " + fieldSpec + ", sort = " + sortSpec + ", limit = " + limit + ", skip = " + skip + "}");

    }
    
    DBCursor getCursor() {
        // Hardcoded to take first url - TODO - Fix me
        return MongoConfigUtil.getCollection(mongoURIs.get(0)).find();
    }


}


