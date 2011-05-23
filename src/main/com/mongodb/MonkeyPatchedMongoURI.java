package com.mongodb;

import com.mongodb.hadoop.util.*;

import java.lang.reflect.*;

/**
 * Copyright (c) 2010, 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

public class MonkeyPatchedMongoURI extends MongoURI {
    /**
     * Creates a MongoURI described by a String. examples mongodb://127.0.0.1 mongodb://fred:foobar@127.0.0.1/
     *
     * @param uri the URI
     *
     * @dochub connections
     */
    public MonkeyPatchedMongoURI( String uri ) {
        super(uri);
        try {
            Field fOptions = MongoURI.class.getDeclaredField("_options");
            fOptions.setAccessible( true );
            MongoOptions tOpts = (MongoOptions) fOptions.get( this );
            tOpts.dbCallbackFactory = SplitFriendlyDBCallback.FACTORY;
        } catch (Exception e) {
            throw new RuntimeException( "Failed to patch new Callback Factory for sharding", e );
        }
    }
}
