package com.mongodb.hadoop.util;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.lang.String.format;

public class MongoClientURIBuilder {
    private MongoClientOptions options;
    private MongoCredential credentials;
    private String host = "localhost";
    private Integer port = 27017;
    private String database;
    private String collection;
    private String userName;
    private String password;
    private Map<String, String> params = new LinkedHashMap<String, String>();

    public MongoClientURIBuilder() {
    }

    public MongoClientURIBuilder(final MongoClientURI mongoClientURI) {
        List<String> hosts = mongoClientURI.getHosts();
        if (!hosts.isEmpty()) {
            String first = hosts.get(0);
            if (first.contains("/")) {
                first = first.substring(0, first.indexOf("/"));
            }
            if (first.contains(":")) {
                String[] split = first.split(":");
                host = split[0];
                port = Integer.valueOf(split[1]);
            } else {
                host = first;
            }
        }
        database = mongoClientURI.getDatabase();
        collection = mongoClientURI.getCollection();
        userName = mongoClientURI.getUsername();
        if (mongoClientURI.getPassword() != null) {
            password = new String(mongoClientURI.getPassword());
        }
        String uri = mongoClientURI.getURI();
        if (uri.contains("?")) {
            String query = uri.substring(uri.indexOf('?') + 1);
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] split = pair.split("=");
                param(split[0], split[1]);
            }
        }
    }

    public MongoClientURIBuilder host(final String host) {
        if (host.contains(":")) {
            String[] split = host.split(":");
            this.host = split[0];
            port = Integer.valueOf(split[1]);
        } else {
            this.host = host;
        }
        return this;
    }

    public MongoClientURIBuilder port(final Integer port) {
        this.port = port;
        return this;
    }

    public MongoClientURIBuilder collection(final String database, final String collection) {
        this.database = database;
        this.collection = collection;
        return this;
    }

    public MongoClientURIBuilder auth(final String userName, final String password) {
        this.userName = userName;
        this.password = password;
        params.put("authSource", "admin");
        return this;
    }

    public MongoClientURIBuilder options(final MongoClientOptions options) {
        this.options = options;
        return this;
    }

    public MongoClientURIBuilder param(final String key, final String value) {
        this.params.put(key, value);
        return this;
    }

    public MongoClientURI build() {
        StringBuilder uri = new StringBuilder(format("mongodb://"));
        if (userName != null) {
            uri.append(format("%s:%s@", userName, password));
        }
        uri.append(format("%s:%d", host, port));
        if (database != null) {
            uri.append(format("/%s.%s", database, collection));

        }
        if (!params.isEmpty()) {
            boolean paramAdded = false;
            for (Entry<String, String> entry : params.entrySet()) {
                uri.append(paramAdded ? "&" : "?");
                paramAdded = true;
                uri.append(format("%s=%s", entry.getKey(), entry.getValue()));
            }
        }
        return new MongoClientURI(uri.toString());
    }

    public MongoClientURIBuilder readPreference(final ReadPreference readPreference) {
        if (!readPreference.equals(ReadPreference.primary())) {
            params.put("readPreference", readPreference.toString());
        } else {
            params.remove("readPreference");
        }
        return this;
    }
}
