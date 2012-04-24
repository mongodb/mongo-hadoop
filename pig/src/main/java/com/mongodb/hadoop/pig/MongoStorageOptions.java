package com.mongodb.hadoop.pig;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

public class MongoStorageOptions {
    
    private ArrayList<MongoStorageOptions.Index> indexes;
    private Update update;
    
    public static final Pattern updateRegex = Pattern.compile("(update|multi)\\s*\\[(.*)\\]");
    public static final Pattern indexRegex = Pattern.compile("\\{(.*)\\}\\s*,\\s*\\{(.*)\\}");
    public static final Pattern keyValueRegex = Pattern.compile("(\\w*)\\s*:\\s*([-]?\\w*)\\s*");
    
    // Private constructor so you must use factory
    private MongoStorageOptions() {
    }
    
    public static class Index {
        public DBObject index;
        public DBObject options;
    }
    
    public static class Update {
        public String[] keys;
        public boolean multi;
    }
    
    public static MongoStorageOptions parseArguments(final String[] args) throws ParseException {
        MongoStorageOptions parser = new MongoStorageOptions();
        parser.indexes = new ArrayList<MongoStorageOptions.Index>();
        for (String arg : args) {
            Matcher upMatch = updateRegex.matcher(arg);
            if (upMatch.matches()) {
                parser.update = new Update();
                parseUpdate(upMatch, parser.update);
                continue;
            }
            
            Matcher indexMatch = indexRegex.matcher(arg);
            if (indexMatch.matches()) {
                Index i = new Index();
                parseIndex(indexMatch, i);
                parser.indexes.add(i);
            } else {
                throw new ParseException("Error parsing argument: " + arg, 0);
            }
        }
        
        return parser;
    }
    
    private static void parseUpdate(final Matcher match, final Update u) {
        u.multi = match.group(1).equals("multi");
        u.keys = match.group(2).split(",");
        for (int i = 0; i < u.keys.length; i++) {
            u.keys[i] = u.keys[i].trim();
        }
    }
    
    private static void parseIndex(final Matcher match, final Index i) {
        // Build our index object
        i.index = new BasicDBObject();
        String index = match.group(1);
        Matcher indexKeys = keyValueRegex.matcher(index);
        while (indexKeys.find()) {
            i.index.put(indexKeys.group(1), Integer.parseInt(indexKeys.group(2)));
        }
        
        // Build our options object
        i.options = new BasicDBObject();
        String options = match.group(2);
        Matcher optionsKeys = keyValueRegex.matcher(options);
        while (optionsKeys.find()) {
            String value = optionsKeys.group(2);
            i.options.put(optionsKeys.group(1), Boolean.parseBoolean(value));
        }
        
        if (!i.options.containsField("sparse")) {
            i.options.put("sparse", false);
        }
        if (!i.options.containsField("unique")) {
            i.options.put("unique", false);
        }
        if (!i.options.containsField("dropDups")) {
            i.options.put("dropDups", false);
        }
        if (!i.options.containsField("background")) {
            i.options.put("background", false);
        }
    }
    
    public MongoStorageOptions.Index[] getIndexes() {
        Index[] arr = new Index[indexes.size()];
        return indexes.toArray(arr);
    }
    
    public Update getUpdate() {
        return update;
    }
    
    public boolean shouldUpdate() {
        return update != null;
    }
}