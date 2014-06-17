package com.mongodb.hadoop.bookstore;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.BSONFileInputFormat;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.testutils.MapReduceJob;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

public class BookstoreTest extends BaseHadoopTest {
    private static final Logger LOG = LoggerFactory.getLogger(BookstoreTest.class);
    public static final URI INVENTORY_BSON;


    private static final File JAR_PATH;

    static {
        try {
            File home = new File(PROJECT_HOME, "core");
            URL resource = BookstoreTest.class.getResource("/bookstore-dump/inventory.bson");
            INVENTORY_BSON = resource.toURI();
            JAR_PATH = findProjectJar(home, true);
            LOG.info("CLUSTER_VERSION = " + CLUSTER_VERSION);
            LOG.info("HADOOP_HOME = " + HADOOP_HOME);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void tagsIndex() throws URISyntaxException, UnknownHostException {
        MongoClientURI uri = authCheck(new MongoClientURIBuilder()
                                           .collection("mongo_hadoop", "bookstore_tags")
                                      ).build();
        MongoClient mongoClient = new MongoClient(uri);
        DBCollection collection = mongoClient.getDB(uri.getDatabase())
                                             .getCollection(uri.getCollection());
        
        final URI outputUri = new URI(uri.getURI());
        new MapReduceJob(BookstoreConfig.class.getName())
            .jar(JAR_PATH)
            .inputUris(INVENTORY_BSON)
            .inputFormat(BSONFileInputFormat.class)
            .outputUris(outputUri)
            .param("mapred.input.dir", INVENTORY_BSON.toString())
            .execute(false);

        DBObject object = collection.findOne(new BasicDBObject("_id", "history"));

        List books = (List) object.get("books");
        Assert.assertEquals("Should find only 8 books", books.size(), 8);
    }
}
