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
package com.mongodb.hadoop.examples.enron;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

import java.io.IOException;

public class EnronMailReducer extends Reducer<MailPair, IntWritable, BSONWritable, IntWritable> {

    private static final Log LOG = LogFactory.getLog(EnronMailReducer.class);

    @Override
    public void reduce(final MailPair pKey,
                       final Iterable<IntWritable> pValues,
                       final Context pContext)
        throws IOException, InterruptedException {
        int sum = 0;
        for (final IntWritable value : pValues) {
            sum += value.get();
        }
        BSONObject outDoc = BasicDBObjectBuilder.start().add("f", pKey.from).add("t", pKey.to).get();
        BSONWritable pkeyOut = new BSONWritable(outDoc);

        pContext.write(pkeyOut, new IntWritable(sum));
    }

}

