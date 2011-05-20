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
package com.mongodb.hadoop.examples;

// Mongo
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

// Commons
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

// Java
import java.io.IOException;

/**
 * The world development indicator mapper.
 */
public class WorldDevIndicatorMapper
    extends Mapper<ObjectId, BSONObject, Text, DoubleWritable>
{
    @Override
    public void map(    final ObjectId pKey,
                        final BSONObject pValue,
                        final Context pContext )
        throws IOException, InterruptedException
    {
        // TODO: Look at configuration to determine what series code/name
        // and see if there is a country/countries filter.

        // Determine the average growth for each country - historical data.

        // pContext.write( country code(s), value ) );
    }

    private static final Log LOG = LogFactory.getLog( WorldDevIndicatorMapper.class );
}

