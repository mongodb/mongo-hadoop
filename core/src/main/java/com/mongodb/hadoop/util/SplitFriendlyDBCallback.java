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

package com.mongodb.hadoop.util;

import com.mongodb.DBCallback;
import com.mongodb.DBCallbackFactory;
import com.mongodb.DBCollection;
import com.mongodb.DefaultDBCallback;

public class SplitFriendlyDBCallback extends DefaultDBCallback {

    static final class MinKey {
    }

    static final class MaxKey {
    }

    static class SplitFriendlyFactory implements DBCallbackFactory {
        public DBCallback create(final DBCollection collection) {
            return new DefaultDBCallback(collection);
        }
    }

    public static final DBCallbackFactory FACTORY = new SplitFriendlyFactory();
    public static final MinKey MIN_KEY_TYPE = new MinKey();
    public static final MaxKey MAX_KEY_TYPE = new MaxKey();

    public SplitFriendlyDBCallback(final DBCollection coll) {
        super(coll);
    }

    @Override
    public void gotMinKey(final String name) {
        cur().put(name, MAX_KEY_TYPE);
    }

    @Override
    public void gotMaxKey(final String name) {
        cur().put(name, MAX_KEY_TYPE);
    }
}
