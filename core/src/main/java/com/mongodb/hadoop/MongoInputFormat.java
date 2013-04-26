/*
 * Copyright 2011 10gen Inc.
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

package com.mongodb.hadoop;

// Mongo

import com.mongodb.*;
import com.mongodb.hadoop.input.*;
import com.mongodb.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;

import java.util.*;

// Commons
// Hadoop
// Java

public class MongoInputFormat extends InputFormat<Object, BSONObject> {

    @Override
    public RecordReader<Object, BSONObject> createRecordReader( InputSplit split, TaskAttemptContext context ) {
        if ( !( split instanceof MongoInputSplit ) )
            throw new IllegalStateException( "Creation of a new RecordReader requires a MongoInputSplit instance." );

        final MongoInputSplit mis = (MongoInputSplit) split;

        return new com.mongodb.hadoop.input.MongoRecordReader( mis );
    }

    @Override
    public List<InputSplit> getSplits( JobContext context ){
        final Configuration hadoopConfiguration = context.getConfiguration();
        final MongoConfig conf = new MongoConfig( hadoopConfiguration );
        return MongoSplitter.calculateSplits( conf );
    }


    public boolean verifyConfiguration( Configuration conf ){
        return true;
    }

    private static final Log LOG = LogFactory.getLog( MongoInputFormat.class );
}
