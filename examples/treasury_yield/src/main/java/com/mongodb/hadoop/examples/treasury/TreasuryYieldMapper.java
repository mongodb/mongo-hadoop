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
package com.mongodb.hadoop.examples.treasury;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.Date;

/**
 * The treasury yield mapper.
 */
public class TreasuryYieldMapper extends Mapper<Object, BSONObject, IntWritable, DoubleWritable>
    implements org.apache.hadoop.mapred.Mapper<Object, BSONWritable, IntWritable, DoubleWritable> {

    private final IntWritable keyInt;
    private final DoubleWritable valueDouble;

    public TreasuryYieldMapper() {
        super();
        keyInt = new IntWritable();
        valueDouble = new DoubleWritable();
    }

    @Override
    @SuppressWarnings("deprecation")
    public void map(final Object pKey, final BSONObject pValue, final Context pContext) throws IOException, InterruptedException {
        keyInt.set(((Date) pValue.get("_id")).getYear() + 1900);
        valueDouble.set(((Number) pValue.get("bc10Year")).doubleValue());
        pContext.write(keyInt, valueDouble);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void map(final Object key, final BSONWritable value, final OutputCollector<IntWritable, DoubleWritable> output,
                    final Reporter reporter)
        throws IOException {
        BSONObject pValue = value.getDoc();
        keyInt.set(((Date) pValue.get("_id")).getYear() + 1900);
        valueDouble.set(((Number) pValue.get("bc10Year")).doubleValue());
        output.collect(keyInt, valueDouble);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf job) {
    }
}

