/*
 * Copyright 2010-2013 10gen Inc.
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

package com.mongodb.hadoop.splitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.List;

public abstract class MongoSplitter {

    //CHECKSTYLE:OFF
    protected Configuration conf;
    //CHECKSTYLE:ON

    public MongoSplitter() {
    }

    public MongoSplitter(final Configuration conf) {
        this.conf = conf;
    }

    public void setConfiguration(final Configuration conf) {
        this.conf = conf;
    }

    public abstract List<InputSplit> calculateSplits() throws SplitFailedException;

}
