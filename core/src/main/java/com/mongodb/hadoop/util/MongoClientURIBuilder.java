package com.mongodb.hadoop.util;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.lang.String.format;

public class MongoClientURIBuilder {
    private MongoClientOptions options;
    private MongoCredential credentials;
    private final List<String> hosts = new ArrayList<String>();
    private String database;
    private String collection;
    private String userName;
    private String password;
    private Map<String, String> params = new LinkedHashMap<String, String>();

    public MongoClientURIBuilder() {
    }

    public MongoClientURIBuilder(final MongoClientURI mongoClientURI) {
        List<String> list = mongoClientURI.getHosts();
        for (String s : list) {
            host(s);
        }
        database = mongoClientURI.getDatabase();
        collection = mongoClientURI.getCollection();
        userName = mongoClientURI.getUsername();
        if (mongoClientURI.getPassword() != null) {
            password = new String(mongoClientURI.getPassword());
        }
        options = mongoClientURI.getOptions();
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
        return host(host, null);
    }

    public MongoClientURIBuilder host(final String newHost, final Integer newPort) {
        hosts.clear();
        addHost(newHost, newPort);

        return this;
    }

    public void addHost(final String newHost, final Integer newPort) {
        if (newHost.contains(":") && newPort == null) {
            hosts.add(newHost);
        } else {
            String host = newHost.isEmpty() ? "localhost" : newHost;
            Integer port = newPort == null ? 27017 : newPort;
            if (host.contains(":")) {
                host = host.substring(0, host.indexOf(':'));
            }
            hosts.add(format("%s:%d", host, port));
        }
    }

    public MongoClientURIBuilder port(final Integer port) {
        if (hosts.size() == 0) {
            host("localhost", port);
        } else {
            host(hosts.get(0), port);
        }
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
        params.put(key, value);
        return this;
    }

    public MongoClientURI build() {
        StringBuilder uri = new StringBuilder(format("mongodb://"));
        if (userName != null) {
            uri.append(format("%s:%s@", userName, password));
        }
        // Use localhost by default if no host is provided.
        if (hosts.isEmpty()) {
            uri.append("localhost");
        } else {
            for (int i = 0; i < hosts.size(); i++) {
                final String host = hosts.get(i);
                if (i != 0) {
                    uri.append(",");
                }
                uri.append(host);
            }
        }
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
