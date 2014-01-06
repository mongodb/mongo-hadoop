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

package com.mongodb.flume;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

// Using SLF4J per https://issues.cloudera.org/browse/FLUME-309

public class MongoDBSink extends EventSink.Base {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSink.class);

    //CHECKSTYLE:OFF
    final MongoURI _uri;
    Mongo _mongoConn;
    DBCollection _collection;
    //CHECKSTYLE:ON

    /**
     * Constructs a new instance against the given URI
     */
    public MongoDBSink(final String uriString) {
        _uri = new MongoURI(uriString);
    }

    @Override
    public void open() {
        try {
            _mongoConn = new Mongo(_uri);
        } catch (final Exception e) {
            LOG.error("Connecting to MongoDB failed.", e);
            throw new MongoException("Failed to connect to MongoDB. ", e);
        }
        try {
            _collection = _uri.connectCollection(_mongoConn);
        } catch (final Exception e) {
            LOG.error("Connected to MongoDB but failed in acquiring collection.", e);
            throw new MongoException("Could not acquire specified collection.", e);
        }
    }

    @Override
    public void append(final Event e) throws IOException {
        /*
         * TODO - Performance would be best if we wrote directly to BSON here...
         * e.g. Not double converting the timestamp, and skipping string
         * encoding/decoding the message body
         */
        // Would it work to use Timestamp + Nanos + Hostname as the ID or is
        // there still a collision chance?
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start("timestamp", new Date(e.getTimestamp()));
        b.append("nanoseconds", e.getNanos());
        b.append("hostname", e.getHost());
        b.append("priority", e.getPriority().name());
        b.append("message", new String(e.getBody()));
        b.append("metadata", new BasicDBObject(e.getAttrs()));
        _collection.insert(b.get());
    }

    @Override
    public void close() throws IOException {
        // TODO - Flume docs specify all blocking must be kicked during
        // disconnect. Verify we do. No hanging!
        _mongoConn.close();
    }

    public static SinkBuilder builder() {
        return new SinkBuilder() {
            // Create a new sink using a MongoDB URI
            @Override
            public EventSink build(final Context context, final String... argv) {
                Preconditions
                    .checkArgument(argv.length == 1,
                                   "usage: mongoDBSink(\"mongodb://[username:password@]host1[:port1][,host2[:port2],...[,"
                                   + "hostN[:portN]]][/[database][?options]]\")\n ... See "
                                   + "http://www.mongodb.org/display/DOCS/Connections for information on the MongoDB Connection" 
                                   + " URI Format."
                                   + "\n\t Note that using [?options] you can specify Write Concern related settings: "
                                   + "\n\t\t safe={true|false} (default: false) Whether or not the driver should send getLastError to "
                                   + "verify each write operation."
                                   + "\n\t\t w={n} (default: 0) Specify the number of servers to replicate a write to before returning"
                                   + " success. When non-zero, implies safe=true."
                                   + "\n\t\t wtimeout={ms} (default: wait forever) The number of milliseconds to wait for W "
                                   + "replications to complete.  When non-zero, implies safe=true."
                                   + "\n\t\t fsync={true|false} (default: false) When enabled, "
                                   + "forces an fsync after each write operation to increase durability.  You probably *don't* want to "
                                   + "do this; see the MongoDB docs for info.  When 'true', implies safe=true");

                return new MongoDBSink(argv[0]);
            }
        };
    }

    /**
     * Special function used by Flume's SourceFactory to pull this class in and use it as a plugin Sink
     */
    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
        List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
        builders.add(new Pair<String, SinkBuilder>("mongoDBSink", builder()));
        return builders;
    }
}
