// MongoInputSplit.java

package com.mongodb.hadoop.input;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.bson.*;
import com.mongodb.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.input.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoInputSplit extends InputSplit implements Writable {
    private static final Log log = LogFactory.getLog(MongoInputSplit.class);

    private Configuration _conf;

    public MongoInputSplit() {
        _conf = null;
    }

    public MongoInputSplit(Configuration config) {
        _conf = config;
    }
    
    public long getLength(){
        // TODO
        return 100;
    }
    
    public String[] getLocations() {
        // TODO
        return new String[]{};// "localhost" };
    }
    
    public void write(DataOutput out)
        throws IOException {
        _conf.write( out );
    }

    public void readFields(DataInput in) throws IOException {
        if ( _conf != null )
            throw new IllegalStateException( "_conf should be null when readFields called" );
        _conf = new Configuration();
        _conf.readFields(in);
        //_conf = new Configuration(in.readLine());
    }

    DBCursor cursor(){
        return MongoConfigUtil.getInputCollection(_conf).find().limit( MongoConfigUtil.getLimit(_conf) );
    }

}


