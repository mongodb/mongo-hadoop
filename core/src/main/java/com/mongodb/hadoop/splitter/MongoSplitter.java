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

import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.List;

public abstract class MongoSplitter {

    private Configuration configuration;

    public MongoSplitter() {
    }

    public MongoSplitter(final Configuration configuration) {
        setConfiguration(configuration);
    }

    public void setConfiguration(final Configuration conf) {
        configuration = conf;
    }

    public abstract List<InputSplit> calculateSplits() throws SplitFailedException;

    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Get a list of nonempty input splits only.
     *
     * @param splits a list of input splits
     * @return a new list of nonempty input splits
     */
    public static List<InputSplit> filterEmptySplits(
      final List<InputSplit> splits) {
        List<InputSplit> results = new ArrayList<InputSplit>(splits.size());
        for (InputSplit split : splits) {
            MongoInputSplit mis = (MongoInputSplit) split;
            if (mis.getCursor().hasNext()) {
                results.add(mis);
            } else {
                MongoConfigUtil.close(
                  mis.getCursor().getCollection().getDB().getMongo());
            }
        }
        return results;
    }
}
