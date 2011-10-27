package com.mongodb.flume;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * Created by IntelliJ IDEA.
 * User: ayakushev
 * Date: 8/30/11
 * Time: 12:21 PM
 */
public class BucketedMongoDBSink extends EventSink.Base {

    static final Logger LOG = LoggerFactory.getLogger(BucketedMongoDBSink.class);

    private final SimpleLRUMap<String, MongoDBSink> mongoWriters = new SimpleLRUMap<String, MongoDBSink>(10,
            new SimpleLRUMap.OnRemove<MongoDBSink>() {
                public void removed(MongoDBSink obj) {
                    try {
                        closeWriter(obj);
                    } catch (IOException e) {
                        LOG.warn("Failed to close weriter: " + obj._uri);
                    }
                }
            });

    private boolean shouldSub = false;
    private MongoDBSink singleWriter = null;
    private String formatUrl;

    public BucketedMongoDBSink(String formatUrl) {
        this.formatUrl = formatUrl;
        shouldSub = Event.containsTag(formatUrl);
    }

    public void append(Event e) throws IOException, InterruptedException {
        MongoDBSink w = singleWriter;
        if (shouldSub) {
            String realUrl = e.escapeString(formatUrl);
            w = mongoWriters.get(realUrl);
            if (w == null) {
                w = openWriter(realUrl);
                mongoWriters.put(realUrl, w);
            }
        }
        w.append(e);
        super.append(e);
    }

    protected MongoDBSink openWriter(String url) throws IOException {
        LOG.info("Opening " + url);
        MongoDBSink w = new MongoDBSink(url);
        w.open();
        return w;
    }

    @Override
    public void open() throws IOException {
        if (!shouldSub) {
            singleWriter = openWriter(formatUrl);
        }
    }

    protected void closeWriter(MongoDBSink writer) throws IOException {
        LOG.info("Closing writer " + writer._uri);
        writer.close();
    }

    @Override
    public void close() throws IOException {
        if (shouldSub) {
            mongoWriters.clear();
        } else {
            closeWriter(singleWriter);
            singleWriter = null;
        }
    }

    public static SinkBuilder builder() {
        return new SinkBuilder() {

            @Override
            public EventSink build(Context context, String... args) {
                Preconditions
                        .checkArgument(
                                args.length == 1,
                                "usage: mongoDBSink(\"mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]\")"
                                        + "\n ... See http://www.mongodb.org/display/DOCS/Connections for information on the MongoDB Connection URI Format."
                                        + "\n\t Note that using [?options] you can specify Write Concern related settings: "
                                        + "\n\t\t safe={true|false} (default: false) Whether or not the driver should send getLastError to verify each write operation."
                                        + "\n\t\t w={n} (default: 0) Specify the number of servers to replicate a write to before returning success. When non-zero, implies safe=true."
                                        + "\n\t\t wtimeout={ms} (default: wait forever) The number of milliseconds to wait for W replications to complete.  When non-zero, implies safe=true."
                                        + "\n\t\t fsync={true|false} (default: false) When enabled, forces an fsync after each write operation to increase durability.  You probably *don't* want to do this; see the MongoDB docs for info.  When 'true', implies safe=true");
                return new BucketedMongoDBSink(args[0]);
            }
        };
    }

    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
        List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
        builders.add(new Pair<String, SinkBuilder>("bucketedMongoDBSink", builder()));
        return builders;
    }


    private static class SimpleLRUMap<K, V> {

        private final Map<K, KeyValueWithUsage<K, V>> dataMap;
        private final int capacity;
        private final OnRemove<V> removeCallback;

        public SimpleLRUMap(int capacity, OnRemove<V> removeCallback) {
            this.dataMap = new HashMap<K, KeyValueWithUsage<K, V>>();
            this.capacity = capacity;
            this.removeCallback = removeCallback;
        }

        public V get(K key) {
            KeyValueWithUsage<K, V> usageInfo = dataMap.get(key);
            if (usageInfo != null) {
                usageInfo.timestamp = System.currentTimeMillis();
                return usageInfo.value;
            }
            return null;
        }

        public V put(K key, V value) {
            KeyValueWithUsage<K, V> oldValue = dataMap.put(key, new KeyValueWithUsage<K, V>(key, value));
            if (dataMap.size() > capacity) {
                expireLRU();
            }
            return oldValue != null ? oldValue.value : null;
        }

        public V remove(K key) {
            KeyValueWithUsage<K, V> removed = dataMap.remove(key);
            return onRemove(removed);
        }

        public void clear() {
            Iterator<Map.Entry<K, KeyValueWithUsage<K, V>>> iter = dataMap.entrySet().iterator();
            while (iter.hasNext()) {
                KeyValueWithUsage<K, V> removed = iter.next().getValue();
                iter.remove();
                onRemove(removed);
            }
        }

        private V onRemove(KeyValueWithUsage<K, V> removed) {
            if (removed != null) {
                removeCallback.removed(removed.value);
                return removed.value;
            }
            return null;
        }

        private void expireLRU() {
            List<KeyValueWithUsage<K, V>> valueWithUsages = new ArrayList<KeyValueWithUsage<K, V>>(dataMap.values());
            Collections.sort(valueWithUsages);
            KeyValueWithUsage<K, V> toExpire = valueWithUsages.get(0);
            remove(toExpire.key);
        }

        public static interface OnRemove<T> {
            public void removed(T obj);
        }

        private static class KeyValueWithUsage<K, V> implements Comparable<KeyValueWithUsage> {

            public K key;
            public V value;
            public long timestamp;

            public KeyValueWithUsage(K key, V value) {
                this.key = key;
                this.value = value;
                this.timestamp = System.currentTimeMillis();
            }

            public int compareTo(KeyValueWithUsage other) {
                return timestamp >= other.timestamp ? (timestamp != other.timestamp ? 1 : 0) : -1;
            }
        }
    }


}
