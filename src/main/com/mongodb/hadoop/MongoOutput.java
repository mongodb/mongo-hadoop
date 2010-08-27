// MongoOutput.java

package com.mongodb.hadoop;

import com.mongodb.*;

import org.apache.hadoop.io.*;

public interface MongoOutput {
    public void appendAsKey( DBObject o );
    public void appendAsValue( DBObject o );
}
