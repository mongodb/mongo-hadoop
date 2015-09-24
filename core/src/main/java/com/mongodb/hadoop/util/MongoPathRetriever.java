package com.mongodb.hadoop.util;

import org.bson.BSONObject;

import java.util.List;

/**
 * Utility class providing a mechanism for retrieving data nested within
 * a MongoDB document.
 */
public final class MongoPathRetriever {

    private MongoPathRetriever() {}

    /**
     * Returns the Object stored at a given path within a MongoDB
     * document. Returns <code>null</code> if the path is not found.
     *
     * @param document MongoDB document in which to search.
     * @param path Dot-separated path to look up.
     * @return the Object stored at the path within the document.
     */
    public static Object get(final BSONObject document, final String path) {
        String[] parts = path.split("\\.");
        Object o = document;
        for (String part : parts) {
            if (null == o) {
                return null;
            } else if (o instanceof List) {
                try {
                    int index = Integer.parseInt(part);
                    if (((List) o).size() > index && index >= 0) {
                        o = ((List) o).get(index);
                    } else {
                        return null;
                    }
                } catch (NumberFormatException e) {
                    return null;
                }
            } else if (o instanceof BSONObject) {
                o = ((BSONObject) o).get(part);
            } else {
                // Hit a leaf before finding the key we were looking for.
                return null;
            }
        }
        return o;
    }
}
